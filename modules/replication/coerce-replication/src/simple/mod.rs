//! simple::Replicator is a basic system that allows state to be safely replicated
//! across a group of Coerce nodes.
//!
//! When a node attempts to write data, the operation is forwarded to the coordinator node
//! and the coordinator attempts to write it to all nodes in the group. Once all nodes acknowledge
//! the operation by emitting an ACK message back to the coordinator, the coordinator commits the
//! data to storage and tells the rest of the group to do the same.
//!

pub mod consensus;
pub mod error;
pub mod heartbeat;
pub mod read;
pub mod write;

use crate::simple::heartbeat::HeartbeatTick;
use crate::simple::read::Read;
use crate::simple::write::{Mutation, UncommittedMutation, Write};
use crate::storage::{Key, Storage, Value};
use coerce::actor::context::ActorContext;
use coerce::actor::message::{Handler, Message};
use coerce::actor::scheduler::timer::Timer;
use coerce::actor::{Actor, ActorRef, ActorRefErr, IntoActor, LocalActorRef};
use coerce::remote::cluster::group::{Node, NodeGroup, NodeGroupEvent, Subscribe};
use coerce::remote::cluster::node::NodeSelector;
use coerce::remote::system::{NodeId, RemoteActorSystem};

use crate::simple::error::Error;
use std::collections::{HashMap, HashSet, VecDeque};
use std::mem;
use std::time::Duration;
use tokio::sync::oneshot;

pub enum Request<K: Key, V: Value> {
    Read(Read<K, V>),
    Write(Write<K, V>),
}

enum State<S: Storage> {
    None,

    Joining {
        request_buffer: VecDeque<Request<S::Key, S::Value>>,
    },

    Recovering {
        cluster: Cluster<S>,
    },

    Available {
        cluster: Cluster<S>,
        pending_mutations: HashMap<u64, Mutation<S::Key, S::Value>>,
    },

    Leader {
        cluster: Cluster<S>,
        uncommitted_mutations: HashMap<u64, UncommittedMutation<S::Key, S::Value>>,
        heartbeat_timer: Timer,
    },
}

impl<S> Default for State<S> {
    fn default() -> Self {
        Self::None
    }
}

struct Cluster<S: Storage> {
    current_leader: NodeId,
    leader_actor: ActorRef<Replicator<S>>,
    nodes: HashMap<NodeId, Node<Replicator<S>>>,
}

impl<S: Storage> Cluster<S> {
    pub fn update_leader(&mut self, leader_id: NodeId) {
        let leader_actor = self.nodes.get(&leader_id).unwrap().actor.clone();

        self.current_leader = leader_id;
        self.leader_actor = leader_actor;
    }
}

pub struct Replicator<S: Storage> {
    storage: S,
    group: LocalActorRef<NodeGroup<Self>>,
    system: RemoteActorSystem,
    state: State<S>,
}

impl<S: Storage> Replicator<S> {
    pub async fn new(
        name: impl ToString,
        sys: &RemoteActorSystem,
        node_selector: NodeSelector,
        storage: S,
    ) -> Result<LocalActorRef<Replicator<S>>, ActorRefErr> {
        let group_name = name.to_string();
        let group = NodeGroup::builder()
            .group_name(&group_name)
            .node_selector(node_selector)
            .build(sys)
            .await?;

        Self {
            storage,
            group,
            system: sys.clone(),
            state: State::Joining {
                request_buffer: VecDeque::new(),
            },
        }
        .into_actor(
            Some(format!("{}-{}", group_name, sys.node_id())),
            sys.actor_system(),
        )
        .await
    }

    fn is_leader(&self) -> bool {
        match &self.state {
            State::Available { cluster, .. } => self.system.node_id() == cluster.current_leader,
            _ => false,
        }
    }
}

fn start_heartbeat_timer<S: Storage>(ctx: &ActorContext) -> Timer {
    Timer::start(
        ctx.actor_ref::<Replicator<S>>(),
        Duration::from_millis(500),
        HeartbeatTick,
    )
}

#[async_trait]
impl<S: Storage> Actor for Replicator<S> {
    async fn started(&mut self, ctx: &mut ActorContext) {
        let self_ref = self.actor_ref(ctx);
        let _ = self.group.notify(Subscribe::<Self>(self_ref.into()));
    }
}

impl<S: Storage> Cluster<S> {
    pub async fn broadcast<M: Message + Clone>(&self, message: M)
    where
        Replicator<S>: Handler<M>,
    {
        let mut messages = itertools::repeat_n(message, self.nodes.len());
        for node in self.nodes.values() {
            let _ = node.actor.notify(messages.next().unwrap()).await;
        }
    }
}

#[async_trait]
impl<S: Storage> Handler<NodeGroupEvent<Replicator<S>>> for Replicator<S> {
    async fn handle(&mut self, message: NodeGroupEvent<Replicator<S>>, ctx: &mut ActorContext) {
        match message {
            NodeGroupEvent::MemberUp { leader_id, nodes } => {
                debug!(leader_id = leader_id, node_count = nodes.len(), "member up");

                let is_leader = leader_id == self.system.node_id();
                let (mut leader_actor) = is_leader.then(|| ActorRef::from(self.actor_ref(ctx)));

                let mut node_map = HashMap::new();
                for node in nodes {
                    if node.node_id == leader_id {
                        leader_actor = Some(node.actor.clone());
                    }

                    node_map.insert(node.node_id, node);
                }

                let cluster = Cluster {
                    current_leader: leader_id,
                    leader_actor: leader_actor.unwrap(),
                    nodes: node_map,
                };

                let new_state = if is_leader {
                    State::Leader {
                        cluster,
                        uncommitted_mutations: Default::default(),
                        heartbeat_timer: start_heartbeat_timer::<S>(ctx),
                    }
                } else {
                    State::Available {
                        cluster,
                        pending_mutations: Default::default(),
                    }
                };

                match mem::replace(&mut self.state, new_state) {
                    State::Joining { request_buffer } => {
                        debug!(
                            pending_requests = request_buffer.len(),
                            "processing pending requests"
                        );

                        for req in request_buffer {
                            match req {
                                Request::Read(read) => {
                                    self.handle(read, ctx).await;
                                }

                                Request::Write(_) => {}
                            }
                        }
                    }
                    _ => {}
                }
            }

            NodeGroupEvent::NodeAdded(node) => {
                debug!(node_id = node.node_id, "node added");

                match &mut self.state {
                    State::Available { cluster, .. } | State::Leader { cluster, .. } => {
                        cluster.nodes.insert(node.node_id, node);
                    }
                    _ => {}
                }
            }

            NodeGroupEvent::NodeRemoved(node_id) => {
                debug!(node_id = node_id, "node removed");

                match &mut self.state {
                    State::Available { cluster, .. } | State::Leader { cluster, .. } => {
                        cluster.nodes.remove(&node_id);
                    }
                    _ => {}
                }
            }

            NodeGroupEvent::LeaderChanged(leader_id) => {
                info!(leader_id = leader_id, "leader changed");

                match mem::take(&mut self.state) {
                    State::Leader {
                        mut cluster,
                        uncommitted_mutations,
                        ..
                    } => {
                        cluster.update_leader(leader_id);

                        for (_, mutation) in uncommitted_mutations {
                            mutation.on_completion.notify_err(
                                Error::LeaderChanged {
                                    new_leader_id: leader_id,
                                },
                                &self.system,
                            );
                        }

                        self.state = State::Available {
                            cluster,
                            pending_mutations: Default::default(),
                        }
                    }

                    State::Available { mut cluster, .. } => {
                        cluster.update_leader(leader_id);

                        let is_leader = leader_id == self.system.node_id();
                        if is_leader {
                            self.state = State::Leader {
                                cluster,
                                uncommitted_mutations: Default::default(),
                                heartbeat_timer: start_heartbeat_timer(ctx),
                            };
                        } else {
                            self.state = State::Available {
                                cluster,
                                pending_mutations: Default::default(),
                            }
                        }
                    }

                    _ => {}
                }
            }
        }
    }
}
