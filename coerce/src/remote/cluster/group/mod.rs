use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message};
use crate::actor::system::ActorSystem;
use crate::actor::{
    Actor, ActorId, ActorRef, ActorRefErr, IntoActor, IntoActorId, LocalActorRef, Receiver,
    ToActorId,
};
use crate::remote::cluster::node::{NodeSelector, RemoteNodeRef};
use crate::remote::stream::pubsub::{PubSub, Receive, Subscription};
use crate::remote::stream::system::{ClusterEvent, ClusterMemberUp, SystemEvent, SystemTopic};
use crate::remote::system::{NodeId, RemoteActorSystem};
use crate::remote::RemoteActorRef;
use crate::singleton::factory::SingletonFactory;
use crate::singleton::manager::lease::{LeaseAck, RequestLease};
use crate::singleton::manager::{Manager, SingletonStarted, State};
use chrono::{DateTime, Utc};
use std::cmp::Ordering;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::mem;

pub enum NodeGroupEvent<A: Actor> {
    MemberUp {
        leader_id: NodeId,
        nodes: Vec<Node<A>>,
    },
    NodeAdded(Node<A>),
    NodeRemoved(NodeId),
    LeaderChanged(NodeId),
}

impl<A: Actor> Message for NodeGroupEvent<A> {
    type Result = ();
}

impl<A: Actor> Clone for NodeGroupEvent<A> {
    fn clone(&self) -> Self {
        match &self {
            NodeGroupEvent::MemberUp { leader_id, nodes } => Self::MemberUp {
                leader_id: *leader_id,
                nodes: nodes.clone(),
            },
            NodeGroupEvent::NodeAdded(node) => Self::NodeAdded(node.clone()),
            NodeGroupEvent::NodeRemoved(node_id) => Self::NodeRemoved(*node_id),
            NodeGroupEvent::LeaderChanged(leader_id) => Self::LeaderChanged(*leader_id),
        }
    }
}

pub struct NodeGroup<A: Actor> {
    node_id: NodeId,
    group_name: String,
    nodes: HashMap<NodeId, Node<A>>,
    selector: NodeSelector,
    subscription: Option<Subscription>,
    receivers: Vec<Receiver<NodeGroupEvent<A>>>,
    actor_id_provider: Box<dyn ActorIdProvider>,
    current_group_leader: Option<NodeId>,
    min_node_count: Option<usize>,
}

pub struct Node<A: Actor> {
    pub node_id: NodeId,
    pub actor: ActorRef<A>,
    pub node: RemoteNodeRef,
}

impl<A: Actor> Clone for Node<A> {
    fn clone(&self) -> Self {
        Self {
            node_id: self.node_id,
            actor: self.actor.clone(),
            node: self.node.clone(),
        }
    }
}

#[async_trait]
impl<A: Actor> Actor for NodeGroup<A> {
    async fn started(&mut self, ctx: &mut ActorContext) {
        self.subscription = Some(
            PubSub::subscribe::<Self, _>(SystemTopic, &ctx)
                .await
                .expect("system subscription"),
        );
    }
}

impl<A: Actor> NodeGroup<A> {
    pub async fn new(
        group_name: impl ToString,
        actor_id_provider: impl ActorIdProvider,
        selector: NodeSelector,
        receiver: Receiver<NodeGroupEvent<A>>,
        system: &RemoteActorSystem,
    ) -> Result<LocalActorRef<Self>, ActorRefErr> {
        let group_name = group_name.to_string();
        Self {
            group_name: group_name.clone(),
            node_id: system.node_id(),
            selector,
            nodes: Default::default(),
            subscription: None,
            receivers: vec![receiver],
            actor_id_provider: Box::new(actor_id_provider),
            current_group_leader: None,
            min_node_count: None,
        }
        .into_actor(Some(group_name.into_actor_id()), system.actor_system())
        .await
    }
}

impl<A: Actor> NodeGroup<A> {
    /// Finds the oldest node in the group, if there are multiple nodes with the same start time,
    /// the node with the lowest ID is returned.
    pub fn leader_id(&self) -> Option<NodeId> {
        let mut nodes: Vec<&Node<_>> = self.nodes.values().collect();
        nodes.sort_by(|a, b| {
            match Ord::cmp(
                &a.node.node_started_at.unwrap_or(DateTime::<Utc>::MIN_UTC),
                &b.node.node_started_at.unwrap_or(DateTime::<Utc>::MIN_UTC),
            ) {
                Ordering::Equal => Ord::cmp(&a.node_id, &b.node_id),
                ordering => ordering,
            }
        });

        nodes.iter().map(|n| n.node_id).next()
    }

    pub fn add(&mut self, node: Node<A>) {
        self.nodes.insert(node.node_id, node);
    }

    fn broadcast(&self, event: NodeGroupEvent<A>) {
        let mut events = itertools::repeat_n(event, self.receivers.len());
        for receiver in &self.receivers {
            let _ = receiver.notify(events.next().unwrap());
        }
    }
}

#[async_trait]
impl<A: Actor> Handler<Receive<SystemTopic>> for NodeGroup<A> {
    async fn handle(&mut self, message: Receive<SystemTopic>, ctx: &mut ActorContext) {
        let sys = ctx.system().remote();
        match message.0.as_ref() {
            SystemEvent::Cluster(e) => match e {
                ClusterEvent::MemberUp(ClusterMemberUp { leader_id, nodes }) => {
                    debug!(
                        cluster_leader = leader_id,
                        nodes_len = nodes.len(),
                        "nodegroup received `MemberUp`"
                    );

                    for node in nodes {
                        if !self.selector.includes(node.as_ref()) {
                            continue;
                        }

                        let actor_id = self
                            .actor_id_provider
                            .get_actor_id(self.group_name.as_ref(), node.id);

                        let actor_ref = RemoteActorRef::new(actor_id, node.id, sys.clone()).into();

                        let node = Node::new(node.id, actor_ref, node.clone());

                        self.nodes.insert(node.node_id, node);
                    }

                    let group_leader = self.leader_id();

                    if let Some(group_leader) = group_leader {
                        self.current_group_leader = Some(group_leader);

                        self.broadcast(NodeGroupEvent::MemberUp {
                            leader_id: group_leader,
                            nodes: self
                                .nodes
                                .values()
                                .filter(|n| n.node_id != self.node_id)
                                .cloned()
                                .collect(),
                        })
                    }
                }

                ClusterEvent::NodeAdded(node) => {
                    debug!(node_id = node.id, "node added");
                    let mut event = None;

                    if self.selector.includes(node.as_ref()) {
                        let mut entry = self.nodes.entry(node.id);
                        if let Entry::Vacant(mut entry) = entry {
                            let actor_id = self
                                .actor_id_provider
                                .get_actor_id(self.group_name.as_ref(), node.id);

                            let remote_ref: ActorRef<A> =
                                RemoteActorRef::new(actor_id, node.id, sys.clone()).into();

                            let node = Node::new(node.id, remote_ref, node.clone());

                            if node.node_id != self.node_id {
                                event = Some(NodeGroupEvent::NodeAdded(node.clone()));
                            }

                            entry.insert(node);
                        }
                    }

                    if let Some(event) = event {
                        self.broadcast(event);
                    }
                }

                ClusterEvent::NodeRemoved(node) => {
                    self.nodes.remove(&node.id);
                    debug!(node_id = node.id, "node removed");

                    self.broadcast(NodeGroupEvent::NodeRemoved(node.id));
                }

                _ => {}
            },
        };

        if let Some(leader_id) = self.leader_id() {
            if Some(leader_id) != self.current_group_leader {
                self.broadcast(NodeGroupEvent::LeaderChanged(leader_id));
                self.current_group_leader = Some(leader_id);
            }
        }
    }
}

impl<A: Actor> Node<A> {
    pub fn new(node_id: NodeId, actor: ActorRef<A>, node: RemoteNodeRef) -> Self {
        Self {
            node_id,
            actor,
            node,
        }
    }
}

pub trait ActorIdProvider: 'static + Sync + Send {
    fn get_actor_id(&self, node_group: &str, node_id: NodeId) -> ActorId;
}

impl<F: Fn(&str, NodeId) -> String + 'static + Sync + Send> ActorIdProvider for F {
    fn get_actor_id(&self, node_group: &str, node_id: NodeId) -> ActorId {
        self(node_group, node_id).into_actor_id()
    }
}
