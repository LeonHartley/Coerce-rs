use futures::SinkExt;
use protobuf::Message as ProtoMessage;
use std::error::Error;
use std::fmt::{Display, Formatter};
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::actor::{ActorId, ActorRefErr};
use crate::remote::actor::{RemoteRequest, RemoteResponse};
use crate::remote::net::message::SessionEvent;
use crate::remote::net::proto::network::ClientResult;
use crate::remote::net::StreamData;
use crate::remote::system::{NodeId, RemoteActorSystem};

#[derive(Debug, Eq, PartialEq)]
pub enum NodeRpcErr {
    NodeUnreachable,
    Serialisation,
    ReceiveFailed,
    Err(ActorRefErr),
}

impl Display for NodeRpcErr {
    fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl Error for NodeRpcErr {}

impl RemoteActorSystem {
    pub async fn node_rpc_proto<T: ProtoMessage>(
        &self,
        message_id: Uuid,
        event: SessionEvent,
        node_id: NodeId,
    ) -> Result<T, NodeRpcErr> {
        match self.node_rpc_raw(message_id, event, node_id).await {
            Ok(res) => match T::parse_from_bytes(&res) {
                Ok(res) => {
                    trace!(target: "NodeEvent", "message_id={}, received result", &message_id);
                    Ok(res)
                }
                Err(_) => {
                    error!(target: "NodeEvent", "message_id={}, failed to decode result from node_id={}", &message_id, &node_id);
                    Err(NodeRpcErr::Serialisation)
                }
            },
            Err(e) => {
                error!(target: "NodeEvent", "failed to receive result, e={:?}", e);
                Err(NodeRpcErr::ReceiveFailed)
            }
        }
    }

    pub async fn node_rpc<T: StreamData>(
        &self,
        message_id: Uuid,
        event: SessionEvent,
        node_id: NodeId,
    ) -> Result<T, NodeRpcErr> {
        match self.node_rpc_raw(message_id, event, node_id).await {
            Ok(res) => match T::read_from_bytes(res) {
                Some(res) => {
                    trace!(target: "NodeRpc", "message_id={}, received result", &message_id);
                    Ok(res)
                }
                None => {
                    error!(target: "NodeRpc", "message_id={}, failed to decode result from node_id={}", &message_id, &node_id);
                    Err(NodeRpcErr::Serialisation)
                }
            },
            _ => {
                error!(target: "NodeRpc", "failed to receive result");
                Err(NodeRpcErr::ReceiveFailed)
            }
        }
    }

    pub async fn node_rpc_raw(
        &self,
        message_id: Uuid,
        event: SessionEvent,
        node_id: NodeId,
    ) -> Result<Vec<u8>, NodeRpcErr> {
        let (res_tx, res_rx) = oneshot::channel();

        trace!(target: "NodeRpc", "message_id={}, created channel, storing request", &message_id);
        self.push_request(message_id, res_tx);

        trace!(target: "NodeRpc", "message_id={}, emitting event to node_id={}", &message_id, &node_id);
        self.notify_node(node_id, event).await;

        trace!(target: "NodeRpc", "message_id={}, waiting for result", &message_id);
        match res_rx.await {
            Ok(RemoteResponse::Ok(res)) => Ok(res),
            Ok(RemoteResponse::Err(res)) => Err(NodeRpcErr::Err(res)),
            Err(e) => {
                error!(target: "NodeEvent", "failed to receive result, e={}", e);
                Err(NodeRpcErr::ReceiveFailed)
            }
        }
    }

    pub async fn notify_raw_rpc_result(&self, request_id: Uuid, result: Vec<u8>, node_id: NodeId) {
        if node_id == self.node_id() {
            let result_sender = self.pop_request(request_id);
            if let Some(result_sender) = result_sender {
                let _ = result_sender.send(RemoteResponse::Ok(result));
            }
        } else {
            let message_id = request_id.to_string();
            let result = SessionEvent::Result(ClientResult {
                message_id,
                result,
                ..Default::default()
            });

            if let Err(e) = self.node_rpc_raw(request_id, result, node_id).await {
                error!(
                    "error whilst sending result to target node (node_id={}, request_id={}) error: {}",
                    &node_id, &request_id, &e
                );
            }
        }
    }

    pub async fn handle_message(
        &self,
        identifier: &str,
        actor_id: ActorId,
        buffer: &[u8],
    ) -> Result<Vec<u8>, ActorRefErr> {
        let (tx, rx) = oneshot::channel();
        let handler = self.inner.config.message_handler(identifier);

        if let Some(handler) = handler {
            handler.handle_attempt(actor_id, buffer, tx, 1).await;
        };

        match rx.await {
            Ok(res) => res,
            Err(_e) => Err(ActorRefErr::ResultChannelClosed),
        }
    }

    pub fn push_request(&self, id: Uuid, res_tx: oneshot::Sender<RemoteResponse>) {
        let mut handler = self.inner.handler_ref.lock();
        handler.push_request(id, RemoteRequest { res_tx });
    }

    pub fn pop_request(&self, id: Uuid) -> Option<oneshot::Sender<RemoteResponse>> {
        let mut handler = self.inner.handler_ref.lock();
        handler.pop_request(id).map(|r| r.res_tx)
    }
    pub fn inflight_remote_request_count(&self) -> usize {
        let handler = self.inner.handler_ref.lock();
        handler.inflight_request_count()
    }
}
