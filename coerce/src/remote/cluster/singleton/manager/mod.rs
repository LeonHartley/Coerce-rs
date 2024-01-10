pub(crate) mod lease;
mod start;
mod status;

use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message};
use crate::actor::{Actor, ActorFactory, ActorId, ActorRef, ActorRefErr, IntoActor, LocalActorRef, ToActorId};
use crate::remote::cluster::node::NodeSelector;
use crate::remote::cluster::singleton::factory::SingletonFactory;
use crate::remote::stream::pubsub::{PubSub, Receive, Subscription};
use crate::remote::stream::system::{ClusterEvent, ClusterMemberUp, SystemEvent, SystemTopic};
use crate::remote::system::{NodeId, RemoteActorSystem};
use crate::remote::RemoteActorRef;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::time::Duration;

pub struct Manager<F: SingletonFactory> {
    system_event_subscription: Option<Subscription>,
    state: State<F::Actor>,
    current_leader_node: Option<NodeId>,
    node_id: NodeId,
    manager_actor_id: ActorId,
    singleton_actor_id: ActorId,
    managers: HashMap<NodeId, ActorRef<Self>>,
    factory: F,
    selector: NodeSelector,
    sys: RemoteActorSystem,
}

impl<F: SingletonFactory> Manager<F> {
    pub fn new(
        sys: RemoteActorSystem,
        factory: F,
        manager_actor_id: ActorId,
        singleton_actor_id: ActorId,
        selector: NodeSelector,
    ) -> Self {
        Self {
            system_event_subscription: None,
            state: State::default(),
            current_leader_node: None,
            node_id: sys.node_id(),
            manager_actor_id,
            singleton_actor_id,
            managers: Default::default(),
            factory,
            selector,
            sys,
        }
    }
}

impl<A: Actor> Default for State<A> {
    fn default() -> Self {
        Self::Idle
    }
}

pub enum State<A: Actor> {
    Idle,
    Starting {
        acknowledged_nodes: HashSet<NodeId>,
    },
    Running {
        actor_ref: LocalActorRef<A>,
    },
    Stopping {
        actor_ref: LocalActorRef<A>,
        lease_requested_by: NodeId,
    },
}

impl<A: Actor> Debug for State<A> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self {
            State::Idle => write!(f, "Idle"),
            State::Starting { .. } => write!(f, "Starting"),
            State::Running { .. } => write!(f, "Running"),
            State::Stopping { .. } => write!(f, "Stopping"),
        }
    }
}

struct Notify<M>(NodeId, M);

impl<M: Message> Message for Notify<M> {
    type Result = ();
}

#[async_trait]
impl<F: SingletonFactory, M: Message + Clone> Handler<Notify<M>> for Manager<F>
where
    Self: Handler<M>,
{
    async fn handle(&mut self, message: Notify<M>, ctx: &mut ActorContext) {
        self.notify_manager(message.0, message.1, ctx).await;
    }
}

impl<A: Actor> State<A> {
    pub fn is_running(&self) -> bool {
        matches!(self, Self::Running { .. } | Self::Starting { .. })
    }

    pub fn get_actor(&self) -> Option<LocalActorRef<A>> {
        match &self {
            Self::Running { actor_ref } | Self::Stopping { actor_ref, .. } => {
                Some(actor_ref.clone())
            }
            _ => None,
        }
    }
}

impl<F: SingletonFactory> Manager<F> {
    pub async fn notify_manager<M: Message + Clone>(
        &self,
        node_id: NodeId,
        message: M,
        ctx: &ActorContext,
    ) where
        Self: Handler<M>,
    {
        if let Some(manager) = self.managers.get(&node_id) {
            info!(
                source_node_id = self.node_id,
                node_id = &node_id,
                message = M::type_name(),
                manager = format!("{:?}", &manager),
                "notifying manager"
            );

            let res = manager.notify(message.clone()).await;
            if res.is_err() {
                let msg = message.clone();
                let self_ref = self.actor_ref(ctx);
                let _ = self_ref.scheduled_notify::<Notify<M>>(
                    Notify::<M>(manager.node_id().unwrap(), msg),
                    Duration::from_secs(2),
                );
            }
        }
    }
    pub async fn notify_managers<M: Message>(&self, message: M, ctx: &ActorContext)
    where
        Self: Handler<M>,
        M: Clone,
    {
        for manager in self.managers.values() {
            info!(
                source_node_id = self.node_id,
                node_id = manager.node_id(),
                message = M::type_name(),
                "notifying manager"
            );

            let res = manager.notify(message.clone()).await;
            if res.is_err() {
                let msg = message.clone();
                let self_ref = self.actor_ref(ctx);
                let _ = self_ref.scheduled_notify::<Notify<M>>(
                    Notify::<M>(manager.node_id().unwrap(), msg),
                    Duration::from_secs(2),
                );
            }
        }
    }

    pub async fn begin_starting(&mut self, ctx: &ActorContext) {
        self.state = State::Starting {
            acknowledged_nodes: HashSet::new(),
        };

        self.request_lease(ctx).await;
    }

    pub fn begin_stopping(&mut self, node_id: NodeId) {
        let actor_ref = self.state.get_actor();
        let actor_ref = match actor_ref {
            Some(actor_ref) => actor_ref,
            None => return,
        };

        let _ = actor_ref.notify_stop();

        self.state = State::Stopping {
            actor_ref,
            lease_requested_by: node_id,
        };
    }

    pub async fn on_leader_changed(&mut self, new_leader_id: NodeId, ctx: &ActorContext) {
        if new_leader_id == self.node_id && !self.state.is_running() {
            self.begin_starting(ctx).await;
        }
    }
}

#[async_trait]
impl<F: SingletonFactory> Actor for Manager<F> {
    async fn started(&mut self, ctx: &mut ActorContext) {
        self.system_event_subscription = Some(
            PubSub::subscribe::<Self, SystemTopic>(SystemTopic, ctx)
                .await
                .unwrap(),
        );

        let sys = ctx.system().remote();
        self.current_leader_node = sys.current_leader();

        info!(
            node_id = self.node_id,
            singleton = F::Actor::type_name(),
            "manager started"
        )
    }

    async fn on_child_stopped(&mut self, id: &ActorId, ctx: &mut ActorContext) {
        if id != &self.singleton_actor_id {
            return;
        }

        match &self.state {
            State::Running { .. } => {
                if !ctx.system().is_terminated() {
                    // TODO: restart actor
                }
            }

            State::Stopping {
                lease_requested_by, ..
            } => {
                self.grant_lease(*lease_requested_by, ctx).await;
                self.state = State::Idle;
            }

            _ => {}
        }
    }
}

#[async_trait]
impl<F: SingletonFactory> Handler<Receive<SystemTopic>> for Manager<F> {
    async fn handle(&mut self, message: Receive<SystemTopic>, ctx: &mut ActorContext) {
        let sys = ctx.system().remote();
        match message.0.as_ref() {
            SystemEvent::Cluster(e) => match e {
                ClusterEvent::MemberUp(ClusterMemberUp {
                    leader_id: leader,
                    nodes,
                }) => {
                    for node in nodes {
                        if node.id == self.node_id || !self.selector.includes(node.as_ref()) {
                            continue;
                        }

                        self.managers.insert(
                            node.id,
                            RemoteActorRef::new(
                                format!("{}-{}", &self.manager_actor_id, node.id).to_actor_id(),
                                node.id,
                                sys.clone(),
                            )
                            .into(),
                        );
                    }

                    if leader == &self.node_id {
                        self.begin_starting(ctx).await;
                    }
                }

                ClusterEvent::LeaderChanged(leader) => {
                    self.on_leader_changed(*leader, ctx).await;
                }

                ClusterEvent::NodeAdded(node) => {
                    if node.id != self.node_id && self.selector.includes(node.as_ref()) {
                        let mut entry = self.managers.entry(node.id);
                        if let Entry::Vacant(mut entry) = entry {
                            let remote_ref = RemoteActorRef::new(
                                format!("{}-{}", &self.manager_actor_id, node.id).to_actor_id(),
                                node.id,
                                sys.clone(),
                            )
                            .into();
                            entry.insert(remote_ref);
                        }
                    }
                }

                ClusterEvent::NodeRemoved(node) => {
                    self.managers.remove(&node.id);

                    // TODO: If we're pending a lease ack from this node - can we re-evaluate now that the node
                    //       has been removed?
                }

                _ => {}
            },
        }
    }
}
