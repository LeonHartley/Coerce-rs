mod lease;
mod status;

use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message};
use crate::actor::{Actor, ActorFactory, ActorId, ActorRef, ActorRefErr, LocalActorRef};
use crate::remote::cluster::node::NodeSelector;
use crate::remote::cluster::singleton::factory::SingletonFactory;
use crate::remote::stream::pubsub::{PubSub, Receive, Subscription};
use crate::remote::stream::system::{ClusterEvent, ClusterMemberUp, SystemEvent, SystemTopic};
use crate::remote::system::{NodeId, RemoteActorSystem};
use crate::remote::RemoteActorRef;
use std::collections::btree_map::Entry;
use std::collections::{HashMap, HashSet};

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

impl<A: Actor> State<A> {
    pub fn is_running(&self) -> bool {
        matches!(self, Self::Running { .. })
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
    pub async fn notify_manager<M: Message>(
        &self,
        node_id: NodeId,
        message: M,
    ) -> Result<(), ActorRefErr>
    where
        Self: Handler<M>,
    {
        if let Some(manager) = self.managers.get(&node_id) {
            manager.notify(message).await
        } else {
            // TODO: WARN
            Err(ActorRefErr::ActorUnavailable)
        }
    }

    pub async fn begin_starting(&mut self, sys: &RemoteActorSystem) {
        self.state = State::Starting {
            acknowledged_nodes: HashSet::new(),
        };

        self.request_lease().await;
    }

    pub fn begin_stopping(&mut self, node_id: NodeId) {
        let actor_ref = self.state.get_actor();
        let actor_ref = match actor_ref {
            Some(actor_ref) => actor_ref,
            None => return,
        };

        self.state = State::Stopping {
            actor_ref,
            lease_requested_by: node_id,
        };

        let _ = actor_ref.notify_stop();
    }

    pub async fn on_leader_changed(&mut self, new_leader_id: NodeId, sys: &RemoteActorSystem) {
        if new_leader_id == sys.node_id() {
            if !self.state.is_running() {
                self.begin_starting(sys).await;
            }
        }
    }

    fn create_remote_ref(&self, node_id: NodeId, sys: &RemoteActorSystem) -> ActorRef<Self> {
        RemoteActorRef::new(self.manager_actor_id.clone(), node_id, sys.clone()).into()
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
                self.grant_lease(*lease_requested_by).await;
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
                ClusterEvent::MemberUp(ClusterMemberUp { leader, nodes }) => {
                    for node in nodes {
                        if !self.selector.includes(node.as_ref()) {
                            continue;
                        }

                        self.managers
                            .insert(node.id, self.create_remote_ref(node.id, &sys));
                    }

                    if leader == self.node_id {
                        // TODO: start
                        self.begin_starting(&sys).await;
                    }
                }

                ClusterEvent::LeaderChanged(leader) => {
                    let sys = ctx.system().remote();

                    self.on_leader_changed(*leader, &sys).await;
                }

                ClusterEvent::NodeAdded(node) => {
                    if self.selector.includes(node.as_ref()) {
                        let entry = self.managers.entry(node.id);
                        if let Entry::Vacant(e) = entry {
                            e.insert(self.create_remote_ref(node.id, &sys));
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
