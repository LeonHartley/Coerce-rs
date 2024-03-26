use crate::simple::error::Error;
use crate::simple::{Replicator, Request, State};
use crate::storage::{Key, Storage, Value};
use coerce::actor::context::ActorContext;
use coerce::actor::message::{
    FromBytes, Handler, Message, MessageUnwrapErr, MessageWrapErr, ToBytes,
};
use coerce::actor::{ActorRef, ActorRefErr};
use coerce::remote::system::{NodeId, RemoteActorSystem};
use protobuf::EnumOrUnknown;
use tokio::sync::oneshot;

pub struct Read<K: Key, V: Value> {
    pub key: K,
    pub on_completion: Option<oneshot::Sender<Result<V, Error>>>,
}

impl<K: Key, V: Value> Message for Read<K, V> {
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

            State::Available { cluster, .. } | State::Recovering { cluster, .. } => {
                let on_completion = message.on_completion.take().unwrap();

                debug!("forwarding request to leader node");

                tokio::spawn(remote_read(
                    cluster.leader_actor.clone(),
                    message.key,
                    on_completion,
                    self.system.clone(),
                ));
            }

            State::Leader { .. } => {
                let on_completion = message.on_completion.take().unwrap();
                let data = self.storage.read(message.key).await;

                debug!("local read, node is leader, emitting result");
                let _ = on_completion.send(data.map_err(|e| e.into()));
            }

            _ => {}
        }
    }
}

pub struct RemoteRead<K: Key> {
    request_id: u64,
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

async fn remote_read<S: Storage>(
    leader_ref: ActorRef<Replicator<S>>,
    key: S::Key,
    on_completion: oneshot::Sender<Result<S::Value, Error>>,
    system: RemoteActorSystem,
) {
    let request_id = system.next_msg_id();
    let (tx, rx) = oneshot::channel();
    system.push_request(request_id, tx);

    debug!(request_id = request_id, "remote read request pushed");

    leader_ref
        .notify(RemoteRead {
            request_id,
            source_node_id: system.node_id(),
            key,
        })
        .await
        .expect("notify leader");

    debug!(request_id = &request_id, "remote leader notified");

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
        request_id = request_id,
        "remote read request result received"
    );

    let _ = on_completion.send(result);
}

#[async_trait]
impl<S: Storage> Handler<RemoteRead<S::Key>> for Replicator<S> {
    async fn handle(&mut self, message: RemoteRead<S::Key>, _ctx: &mut ActorContext) {
        debug!(
            request_id = message.request_id,
            source_node_id = message.source_node_id,
            "received remote read request"
        );

        if !self.is_leader() {
            info!("remote request received but node is not leader, forwarding to leader");
            return;
        }

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
                        request_id = request_id,
                        source_node_id = message.source_node_id,
                        "remote result sent (OK)"
                    );

                    system
                        .notify_raw_rpc_result(request_id, bytes, source_node_id)
                        .await
                }
                Err(e) => {
                    debug!(
                        request_id = request_id,
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

impl<V: Value> ToBytes for RemoteReadResult<V> {
    fn to_bytes(self) -> Result<Vec<u8>, MessageWrapErr> {
        crate::protocol::simple::RemoteReadResult {
            result: match self {
                Self::Ok(v) => Some(crate::protocol::simple::remote_read_result::Result::Value(
                    v.to_bytes()?,
                )),
                Self::Err(_) => Some(crate::protocol::simple::remote_read_result::Result::Error(
                    crate::protocol::simple::Error {
                        error_type: EnumOrUnknown::from(
                            crate::protocol::simple::ErrorType::NOT_READY,
                        ),
                        ..Default::default()
                    },
                )),
            },
            ..Default::default()
        }
        .to_bytes()
    }
}

impl<V: Value> FromBytes for RemoteReadResult<V> {
    fn from_bytes(buf: Vec<u8>) -> Result<Self, MessageUnwrapErr> {
        let proto = crate::protocol::simple::RemoteReadResult::from_bytes(buf)?;
        match proto.result {
            Some(crate::protocol::simple::remote_read_result::Result::Value(v)) => {
                Ok(RemoteReadResult::Ok(V::from_bytes(v)?))
            }
            Some(crate::protocol::simple::remote_read_result::Result::Error(_)) => {
                Ok(RemoteReadResult::Err(Error::NotReady /*Todo: this*/))
            }
            None => Err(MessageUnwrapErr::DeserializationErr),
        }
    }
}

impl<K: Key> Message for RemoteRead<K> {
    type Result = ();

    fn as_bytes(&self) -> Result<Vec<u8>, MessageWrapErr> {
        crate::protocol::simple::RemoteRead {
            request_id: self.request_id,
            source_node_id: self.source_node_id,
            key: self.key.clone().to_bytes()?,
            ..Default::default()
        }
        .to_bytes()
    }

    fn from_bytes(bytes: Vec<u8>) -> Result<Self, MessageUnwrapErr> {
        let proto = crate::protocol::simple::RemoteRead::from_bytes(bytes)?;

        Ok(Self {
            request_id: proto.request_id,
            source_node_id: proto.source_node_id,
            key: K::from_bytes(proto.key)?,
        })
    }
}
