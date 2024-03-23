pub(crate) mod lease;
mod start;
mod status;

use crate::actor::context::ActorContext;
use crate::actor::message::{
    FromBytes, Handler, Message, MessageUnwrapErr, MessageWrapErr, ToBytes,
};
use crate::actor::{
    Actor, ActorFactory, ActorId, ActorRef, IntoActor, IntoActorId, LocalActorRef, ToActorId,
};
use crate::remote::cluster::group::{ActorIdProvider, Node, NodeGroup, NodeGroupEvent};
use crate::remote::cluster::node::{NodeSelector, RemoteNodeRef};
use crate::remote::stream::pubsub::{PubSub, Receive, Subscription};
use crate::remote::stream::system::{ClusterEvent, ClusterMemberUp, SystemEvent, SystemTopic};
use crate::remote::system::{NodeId, RemoteActorSystem};
use crate::remote::RemoteActorRef;
use crate::singleton::factory::SingletonFactory;
use crate::singleton::manager::lease::{LeaseAck, RequestLease};
use crate::singleton::proxy::Proxy;
use crate::singleton::{proto, proxy};
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::mem;
use std::time::Duration;

pub struct Manager<F: SingletonFactory> {
    state: State<F::Actor>,
    node_id: NodeId,
    manager_actor_id: ActorId,
    singleton_actor_id: ActorId,
    group: Option<LocalActorRef<NodeGroup<Self>>>,
    nodes: HashMap<NodeId, ActorRef<Self>>,
    factory: F,
    selector: NodeSelector,
    sys: RemoteActorSystem,
    proxy: LocalActorRef<Proxy<F::Actor>>,
    cluster_up: bool,
}

impl<F: SingletonFactory> Manager<F> {
    pub fn new(
        sys: RemoteActorSystem,
        factory: F,
        manager_actor_id: ActorId,
        singleton_actor_id: ActorId,
        selector: NodeSelector,
        proxy: LocalActorRef<Proxy<F::Actor>>,
    ) -> Self {
        Self {
            state: State::default(),
            node_id: sys.node_id(),
            manager_actor_id,
            singleton_actor_id,
            group: None,
            nodes: HashMap::default(),
            factory,
            selector,
            sys,
            proxy,
            cluster_up: false,
        }
    }
}

struct ManagerActorIdProvider;

impl ActorIdProvider for ManagerActorIdProvider {
    fn get_actor_id(&self, node_group: &str, node_id: NodeId) -> ActorId {
        format!("{}-{}", node_group, node_id).into_actor_id()
    }
}

#[async_trait]
impl<F: SingletonFactory> Actor for Manager<F> {
    async fn started(&mut self, ctx: &mut ActorContext) {
        let self_ref = self.actor_ref(ctx).into();

        self.group = Some(
            NodeGroup::builder()
                .group_name(format!("singleton-manager<{}>", F::Actor::type_name()))
                .receiver(self_ref)
                .node_selector(self.selector.clone())
                .build(&self.sys)
                .await
                .expect("create node group"),
        );

        debug!(
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

impl<A: Actor> Default for State<A> {
    fn default() -> Self {
        Self::Joining {
            acknowledgement_pending: None,
        }
    }
}

pub enum State<A: Actor> {
    Joining {
        acknowledgement_pending: Option<NodeId>,
    },
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
            State::Joining { .. } => write!(f, "Joining"),
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

    pub fn is_joining(&self) -> bool {
        matches!(self, Self::Joining { .. })
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
        if let Some(manager) = self.nodes.get(&node_id) {
            self.notify(message, &manager, ctx).await;
        } else {
            warn!(
                node_id = node_id,
                message = M::type_name(),
                "attempted to notify manager that doesn't exist"
            )
        }
    }

    pub async fn notify_managers<M: Message>(&self, message: M, ctx: &ActorContext)
    where
        Self: Handler<M>,
        M: Clone,
    {
        for manager in self.nodes.values() {
            self.notify(message.clone(), &manager, ctx).await;
        }
    }

    async fn notify<M: Message + Clone>(
        &self,
        message: M,
        manager: &ActorRef<Self>,
        ctx: &ActorContext,
    ) where
        Self: Handler<M>,
    {
        debug!(
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

    pub async fn begin_starting(&mut self, ctx: &ActorContext) {
        self.state = State::Starting {
            acknowledged_nodes: HashSet::new(),
        };

        debug!("state changed to `Starting`");
        self.request_lease(ctx).await;
    }

    pub async fn begin_stopping(&mut self, node_id: NodeId, ctx: &ActorContext) {
        let actor_ref = self.state.get_actor();
        let actor_ref = match actor_ref {
            Some(actor_ref) => actor_ref,
            None => return,
        };

        let _ = actor_ref.notify_stop();
        self.notify_managers(
            SingletonStopped {
                source_node_id: self.node_id,
            },
            ctx,
        )
        .await;

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
impl<F: SingletonFactory> Handler<NodeGroupEvent<Self>> for Manager<F> {
    async fn handle(&mut self, message: NodeGroupEvent<Self>, ctx: &mut ActorContext) {
        match message {
            NodeGroupEvent::MemberUp { leader_id, nodes } => {
                debug!(
                    leader = leader_id,
                    nodes_len = nodes.len(),
                    "manager received `MemberUp`"
                );

                for node in nodes {
                    info!(
                        node_id = node.node_id,
                        actor_id = node.actor.actor_id().as_ref(),
                        "added node"
                    );

                    self.nodes.insert(node.node_id, node.actor);
                }

                match mem::replace(&mut self.state, State::Idle) {
                    State::Joining {
                        acknowledgement_pending: Some(node_id),
                    } => {
                        self.notify_manager(
                            node_id,
                            LeaseAck {
                                source_node_id: self.node_id,
                            },
                            ctx,
                        )
                        .await;
                    }
                    _ => {}
                }

                if leader_id == self.node_id {
                    info!("begin starting");
                    self.begin_starting(ctx).await;
                }
            }

            NodeGroupEvent::LeaderChanged(node_id) => {
                if !self.state.is_joining() {
                    self.on_leader_changed(node_id, ctx).await;
                }
            }

            NodeGroupEvent::NodeAdded(node) => {
                debug!(node_id = node.node_id, "node added");
                let mut entry = self.nodes.entry(node.node_id);

                if let Entry::Vacant(mut entry) = entry {
                    match &self.state {
                        State::Starting { .. } => {
                            let _ = node
                                .actor
                                .notify(RequestLease {
                                    source_node_id: self.node_id,
                                })
                                .await;
                        }
                        State::Running { .. } => {
                            let _ = node
                                .actor
                                .notify(SingletonStarted {
                                    source_node_id: self.node_id,
                                })
                                .await;
                        }

                        _ => {}
                    }

                    entry.insert(node.actor);
                }
            }

            NodeGroupEvent::NodeRemoved(node_id) => {
                self.nodes.remove(&node_id);

                debug!(node_id = node_id, "node removed");
                if !self.state.is_joining() {
                    if let State::Starting { acknowledged_nodes } = &mut self.state {
                        acknowledged_nodes.remove(&node_id);

                        if acknowledged_nodes.len() == self.nodes.len() {
                            self.on_all_managers_acknowledged(ctx).await;
                        }
                    }
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct SingletonStarted {
    source_node_id: NodeId,
}

#[derive(Clone)]
pub struct SingletonStopped {
    source_node_id: NodeId,
}

#[async_trait]
impl<F: SingletonFactory> Handler<SingletonStarted> for Manager<F> {
    async fn handle(&mut self, message: SingletonStarted, ctx: &mut ActorContext) {
        debug!(
            source_node_id = message.source_node_id,
            "received started notification, notifying proxy"
        );

        let actor_ref = RemoteActorRef::new(
            self.singleton_actor_id.clone(),
            message.source_node_id,
            self.sys.clone(),
        )
        .into();

        let _ = self.proxy.notify(proxy::SingletonStarted::new(actor_ref));
    }
}

#[async_trait]
impl<F: SingletonFactory> Handler<SingletonStopped> for Manager<F> {
    async fn handle(&mut self, message: SingletonStopped, ctx: &mut ActorContext) {
        debug!(
            source_node_id = message.source_node_id,
            "received stopped notification, notifying proxy"
        );

        let _ = self.proxy.notify(proxy::SingletonStopping);
    }
}

impl Message for SingletonStarted {
    type Result = ();

    fn as_bytes(&self) -> Result<Vec<u8>, MessageWrapErr> {
        proto::singleton::SingletonStarted {
            source_node_id: self.source_node_id,
            ..Default::default()
        }
        .to_bytes()
    }

    fn from_bytes(buf: Vec<u8>) -> Result<Self, MessageUnwrapErr> {
        proto::singleton::SingletonStarted::from_bytes(buf).map(|l| Self {
            source_node_id: l.source_node_id,
        })
    }

    fn read_remote_result(_: Vec<u8>) -> Result<Self::Result, MessageUnwrapErr> {
        Ok(())
    }

    fn write_remote_result(_res: Self::Result) -> Result<Vec<u8>, MessageWrapErr> {
        Ok(vec![])
    }
}

impl Message for SingletonStopped {
    type Result = ();

    fn as_bytes(&self) -> Result<Vec<u8>, MessageWrapErr> {
        proto::singleton::SingletonStopped {
            source_node_id: self.source_node_id,
            ..Default::default()
        }
        .to_bytes()
    }

    fn from_bytes(buf: Vec<u8>) -> Result<Self, MessageUnwrapErr> {
        proto::singleton::SingletonStopped::from_bytes(buf).map(|l| Self {
            source_node_id: l.source_node_id,
        })
    }

    fn read_remote_result(_: Vec<u8>) -> Result<Self::Result, MessageUnwrapErr> {
        Ok(())
    }

    fn write_remote_result(_res: Self::Result) -> Result<Vec<u8>, MessageWrapErr> {
        Ok(vec![])
    }
}
