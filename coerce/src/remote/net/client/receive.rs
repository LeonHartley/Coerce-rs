use crate::actor::message::Message;

use crate::actor::LocalActorRef;
use crate::remote::actor::RemoteResponse;
use crate::remote::cluster::node::{NodeIdentity, RemoteNode};
use crate::remote::net::client::connect::Disconnected;
use crate::remote::net::client::RemoteClient;
use crate::remote::net::message::{timestamp_to_datetime, ClientEvent};
use crate::remote::net::proto::network::Pong;
use crate::remote::net::{proto, StreamReceiver};
use crate::remote::system::{NodeId, RemoteActorSystem};
use chrono::{DateTime, Utc};
use protobuf::Message as ProtoMessage;

use std::str::FromStr;

use tokio::sync::oneshot::Sender;
use uuid::Uuid;

pub struct ClientMessageReceiver {
    actor_ref: LocalActorRef<RemoteClient>,
    identity_sender: Option<Sender<NodeIdentity>>,
}

impl ClientMessageReceiver {
    pub fn new(
        actor_ref: LocalActorRef<RemoteClient>,
        identity_sender: Sender<NodeIdentity>,
    ) -> ClientMessageReceiver {
        let identity_sender = Some(identity_sender);
        Self {
            actor_ref,
            identity_sender,
        }
    }
}

pub struct HandshakeAcknowledge {
    pub node_id: NodeId,
    pub node_tag: String,
    pub node_started_at: DateTime<Utc>,
    pub known_nodes: Vec<RemoteNode>,
}

impl Message for HandshakeAcknowledge {
    type Result = ();
}

#[async_trait]
impl StreamReceiver for ClientMessageReceiver {
    type Message = ClientEvent;

    async fn on_receive(&mut self, msg: ClientEvent, sys: &RemoteActorSystem) {
        match msg {
            ClientEvent::Identity(identity) => {
                if let Some(identity_sender) = self.identity_sender.take() {
                    let _ = identity_sender.send(NodeIdentity {
                        node: parse_proto_node_identity(&identity),
                        peers: identity.peers.into_iter().map(parse_proto_node).collect(),
                    });
                } else {
                    debug!("received `Identity` but the client was already identified");
                }
            }
            ClientEvent::Handshake(msg) => {
                let node_id = msg.node_id;

                let node_tag = msg.node_tag;
                let node_started_at = msg
                    .node_started_at
                    .into_option()
                    .map_or_else(|| Utc::now(), timestamp_to_datetime);

                let known_nodes = msg
                    .nodes
                    .into_iter()
                    .filter(|n| n.node_id != node_id)
                    .map(parse_proto_node)
                    .collect();

                if !self
                    .actor_ref
                    .send(HandshakeAcknowledge {
                        node_id,
                        node_tag,
                        node_started_at,
                        known_nodes,
                    })
                    .await
                    .is_ok()
                {
                    warn!(target: "RemoteClient", "error sending handshake_tx");
                }
            }
            ClientEvent::Result(res) => {
                match sys.pop_request(Uuid::from_str(&res.message_id).unwrap()) {
                    Some(res_tx) => {
                        let _ = res_tx.send(RemoteResponse::Ok(res.result));
                    }
                    None => {
                        trace!(target: "RemoteClient", "node_tag={}, node_id={}, received unknown request result (id={})",
                            sys.node_tag(),
                            sys.node_id(),
                            res.message_id);
                    }
                }
            }
            ClientEvent::Err(_e) => {}
            ClientEvent::Ping(_ping) => {}
            ClientEvent::Pong(pong) => {
                match sys.pop_request(Uuid::from_str(&pong.message_id).unwrap()) {
                    Some(res_tx) => {
                        let _ = res_tx.send(RemoteResponse::Ok(
                            Pong {
                                message_id: pong.message_id,
                                ..Pong::default()
                            }
                            .write_to_bytes()
                            .expect("serialised pong"),
                        ));
                    }
                    None => {
                        //                                          :P
                        warn!(target: "RemoteClient", "received unsolicited pong");
                    }
                }
            }
        }
    }

    async fn on_close(&mut self, _sys: &RemoteActorSystem) {
        let _ = self.actor_ref.send(Disconnected).await;
    }
}

pub fn parse_proto_node(n: proto::network::RemoteNode) -> RemoteNode {
    RemoteNode {
        id: n.get_node_id(),
        addr: n.addr,
        tag: n.tag,
        node_started_at: n.node_started_at.into_option().map(timestamp_to_datetime),
    }
}

pub fn parse_proto_node_identity(n: &proto::network::NodeIdentity) -> RemoteNode {
    RemoteNode {
        id: n.get_node_id(),
        addr: n.addr.clone(),
        tag: n.node_tag.clone(),
        node_started_at: n
            .node_started_at
            .clone()
            .into_option()
            .map(timestamp_to_datetime),
    }
}
