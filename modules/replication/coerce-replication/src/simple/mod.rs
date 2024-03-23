//! simple::Replicator is a basic system that allows state to be safely replicated
//! across a group of Coerce nodes.
//!
//! When a node attempts to write data, the operation is forwarded to the coordinator node
//! and the coordinator attempts to write it to all nodes in the group. Once all nodes acknowledge
//! the operation by emitting an ACK message back to the coordinator, the coordinator commits the
//! data to storage and tells the rest of the group to do the same.
//!

mod consensus;

use crate::protocol::simple as proto;
use crate::storage::{Key, Storage, StorageErr, Value};
use coerce::actor::context::ActorContext;
use coerce::actor::message::{
    FromBytes, Handler, Message, MessageUnwrapErr, MessageWrapErr, ToBytes,
};
use coerce::actor::{Actor, ActorRef, ActorRefErr, IntoActor, LocalActorRef};
use coerce::remote::cluster::group::{Node, NodeGroup, NodeGroupEvent, Subscribe};
use coerce::remote::cluster::node::NodeSelector;
use coerce::remote::system::{NodeId, RemoteActorSystem};
use protobuf::EnumOrUnknown;
use std::collections::{HashMap, VecDeque};
use std::mem;
use tokio::sync::oneshot;
use uuid::Uuid;

enum State<S: Storage> {
    Joining {
        request_buffer: VecDeque<Request<S::Key, S::Value>>,
    },

    Available {
        current_leader: NodeId,
        leader_actor: ActorRef<Replicator<S>>,
        nodes: HashMap<NodeId, Node<Replicator<S>>>,
    },
}

pub struct Replicator<S: Storage> {
    storage: S,
    group: LocalActorRef<NodeGroup<Self>>,
    system: RemoteActorSystem,
    state: State<S>,
}

#[derive(Debug)]
pub enum Error {
    NotReady,
    Storage(StorageErr),
    ActorRef(ActorRefErr),
    Deserialisation(MessageUnwrapErr),
    Serialisation(MessageWrapErr),
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
            State::Available { current_leader, .. } => self.system.node_id() == *current_leader,
            _ => false,
        }
    }

    fn get_leader(&self) -> Option<ActorRef<Self>> {
        match &self.state {
            State::Available { leader_actor, .. } => Some(leader_actor.clone()),
            _ => None,
        }
    }
}

#[async_trait]
impl<S: Storage> Actor for Replicator<S> {
    async fn started(&mut self, ctx: &mut ActorContext) {
        let self_ref = self.actor_ref(ctx);
        let _ = self.group.notify(Subscribe::<Self>(self_ref.into()));
    }
}

#[async_trait]
impl<S: Storage> Handler<NodeGroupEvent<Replicator<S>>> for Replicator<S> {
    async fn handle(&mut self, message: NodeGroupEvent<Replicator<S>>, ctx: &mut ActorContext) {
        match message {
            NodeGroupEvent::MemberUp { leader_id, nodes } => {
                debug!(leader_id = leader_id, node_count = nodes.len(), "member up");

                let mut leader_actor = if leader_id == self.system.node_id() {
                    Some(ActorRef::from(self.actor_ref(ctx)))
                } else {
                    None
                };

                let mut node_map = HashMap::new();
                for node in nodes {
                    if node.node_id == leader_id {
                        leader_actor = Some(node.actor.clone());
                    }

                    node_map.insert(node.node_id, node);
                }

                let old_state = mem::replace(
                    &mut self.state,
                    State::Available {
                        current_leader: leader_id,
                        leader_actor: leader_actor.unwrap(),
                        nodes: node_map,
                    },
                );

                match old_state {
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
                    State::Available { nodes, .. } => {
                        nodes.insert(node.node_id, node);
                    }
                    _ => {}
                }
            }

            NodeGroupEvent::NodeRemoved(node_id) => {
                debug!(node_id = node_id, "node removed");

                match &mut self.state {
                    State::Available { nodes, .. } => {
                        nodes.remove(&node_id);
                    }
                    _ => {}
                }
            }

            NodeGroupEvent::LeaderChanged(leader_id) => {
                info!(leader_id = leader_id, "leader changed");

                match &mut self.state {
                    State::Available {
                        current_leader,
                        leader_actor,
                        nodes,
                    } => {
                        *current_leader = leader_id;
                        *leader_actor = nodes.get(&leader_id).unwrap().actor.clone();
                    }
                    _ => {}
                }
            }
        }
    }
}

pub enum Request<K: Key, V: Value> {
    Read(Read<K, V>),
    Write(Write<K, V>),
}

pub struct Read<K: Key, V: Value> {
    pub key: K,
    pub on_completion: Option<oneshot::Sender<Result<V, Error>>>,
}

impl<K: Key, V: Value> Message for Read<K, V> {
    type Result = ();
}

pub struct Write<K: Key, V: Value> {
    pub key: K,
    pub value: V,
    pub on_completion: Option<oneshot::Sender<Result<(), Error>>>,
}

impl<K: Key, V: Value> Message for Write<K, V> {
    type Result = ();
}

#[async_trait]
impl<S: Storage> Handler<Read<S::Key, S::Value>> for Replicator<S> {
    async fn handle(&mut self, mut message: Read<S::Key, S::Value>, _ctx: &mut ActorContext) {
        if message.on_completion.as_ref().unwrap().is_closed() {
            debug!("completion channel already closed, exiting");
            return;
        }

        match &mut self.state {
            State::Joining { request_buffer } => {
                request_buffer.push_back(Request::Read(message));

                debug!(
                    pending_requests = request_buffer.len(),
                    "replicator still joining cluster, buffered read request"
                );
            }

            State::Available {
                leader_actor,
                current_leader,
                ..
            } => {
                let on_completion = message.on_completion.take().unwrap();

                if *current_leader == self.system.node_id() {
                    let data = self.storage.read(message.key).await;

                    debug!("local read, node is leader, emitting result");
                    let _ = on_completion.send(data.map_err(|e| e.into()));
                } else {
                    debug!("remote leader, spawning remote_read task");

                    tokio::spawn(remote_read(
                        leader_actor.clone(),
                        message.key,
                        on_completion,
                        self.system.clone(),
                    ));
                }
            }
        }
    }
}

pub struct RemoteRead<K: Key> {
    request_id: Uuid,
    source_node_id: NodeId,
    key: K,
}

pub enum RemoteReadResult<V: Value> {
    Ok(V),
    Err(Error),
}

impl<V: Value> RemoteReadResult<V> {
    fn into_result(self) -> Result<V, Error> {
        match self {
            RemoteReadResult::Ok(value) => Ok(value),
            RemoteReadResult::Err(e) => Err(e),
        }
    }
}

impl<V: Value> ToBytes for RemoteReadResult<V> {
    fn to_bytes(self) -> Result<Vec<u8>, MessageWrapErr> {
        proto::RemoteReadResult {
            result: match self {
                Self::Ok(v) => Some(proto::remote_read_result::Result::Value(v.to_bytes()?)),
                Self::Err(_) => Some(proto::remote_read_result::Result::Error(proto::Error {
                    error_type: EnumOrUnknown::from(proto::ErrorType::NOT_READY),
                    ..Default::default()
                })),
            },
            ..Default::default()
        }
        .to_bytes()
    }
}

impl<V: Value> FromBytes for RemoteReadResult<V> {
    fn from_bytes(buf: Vec<u8>) -> Result<Self, MessageUnwrapErr> {
        let proto = proto::RemoteReadResult::from_bytes(buf)?;
        match proto.result {
            Some(proto::remote_read_result::Result::Value(v)) => {
                Ok(RemoteReadResult::Ok(V::from_bytes(v)?))
            }
            Some(proto::remote_read_result::Result::Error(_)) => {
                Ok(RemoteReadResult::Err(Error::NotReady /*Todo: this*/))
            }
            None => Err(MessageUnwrapErr::DeserializationErr),
        }
    }
}

impl<K: Key> Message for RemoteRead<K> {
    type Result = ();

    fn as_bytes(&self) -> Result<Vec<u8>, MessageWrapErr> {
        proto::RemoteRead {
            request_id: self.request_id.to_string(),
            source_node_id: self.source_node_id,
            key: self.key.clone().to_bytes()?,
            ..Default::default()
        }
        .to_bytes()
    }

    fn from_bytes(bytes: Vec<u8>) -> Result<Self, MessageUnwrapErr> {
        let proto = proto::RemoteRead::from_bytes(bytes)?;

        Ok(Self {
            request_id: Uuid::parse_str(&proto.request_id).unwrap(),
            source_node_id: proto.source_node_id,
            key: K::from_bytes(proto.key)?,
        })
    }
}

async fn remote_read<S: Storage>(
    leader_ref: ActorRef<Replicator<S>>,
    key: S::Key,
    on_completion: oneshot::Sender<Result<S::Value, Error>>,
    system: RemoteActorSystem,
) {
    let request_id = Uuid::new_v4();
    let (tx, rx) = oneshot::channel();
    system.push_request(request_id, tx);

    let request_id_str = request_id.to_string();
    debug!(request_id = &request_id_str, "remote read request pushed");

    leader_ref
        .notify(RemoteRead {
            request_id,
            source_node_id: system.node_id(),
            key,
        })
        .await
        .expect("notify leader");

    debug!(request_id = &request_id_str, "remote leader notified");

    let result = rx
        .await
        .map_err(|_| Error::ActorRef(ActorRefErr::ResultChannelClosed))
        .and_then(|result| {
            result
                .into_result()
                .map_err(Error::ActorRef)
                .and_then(|result| {
                    RemoteReadResult::<S::Value>::from_bytes(result)
                        .map_err(Error::Deserialisation)
                        .and_then(|r| r.into_result())
                })
        });

    debug!(
        request_id = &request_id_str,
        "remote read request result received"
    );

    let _ = on_completion.send(result);
}

#[async_trait]
impl<S: Storage> Handler<RemoteRead<S::Key>> for Replicator<S> {
    async fn handle(&mut self, message: RemoteRead<S::Key>, ctx: &mut ActorContext) {
        let request_id_str = message.request_id.to_string();
        debug!(
            request_id = request_id_str,
            source_node_id = message.source_node_id,
            "received remote read request"
        );

        let request_id = message.request_id;
        let source_node_id = message.source_node_id;
        let read_result = self.storage.read(message.key).await;
        let system = self.system.clone();
        tokio::spawn(async move {
            let result = match read_result {
                Ok(value) => RemoteReadResult::Ok(value),
                Err(e) => RemoteReadResult::Err(e.into()),
            }
            .to_bytes();

            match result {
                Ok(bytes) => {
                    debug!(
                        request_id = request_id_str,
                        source_node_id = message.source_node_id,
                        "remote result sent (OK)"
                    );

                    system
                        .notify_raw_rpc_result(request_id, bytes, source_node_id)
                        .await
                }
                Err(e) => {
                    debug!(
                        request_id = request_id_str,
                        source_node_id = message.source_node_id,
                        "received remote read request (ERR)"
                    );

                    system
                        .notify_rpc_err(request_id, ActorRefErr::Serialisation(e), source_node_id)
                        .await
                }
            };
        });
    }
}

#[async_trait]
impl<S: Storage> Handler<Write<S::Key, S::Value>> for Replicator<S> {
    async fn handle(&mut self, message: Write<S::Key, S::Value>, ctx: &mut ActorContext) {}
}

impl From<StorageErr> for Error {
    fn from(value: StorageErr) -> Self {
        Self::Storage(value)
    }
}
