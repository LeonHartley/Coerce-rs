use crate::actor::message::Message;
use std::io::Error;

use crate::actor::LocalActorRef;
use crate::remote::actor::RemoteResponse;
use crate::remote::cluster::node::{NodeIdentity, RemoteNode};
use crate::remote::net::client::connect::Disconnected;
use crate::remote::net::client::RemoteClient;
use crate::remote::net::message::{timestamp_to_datetime, ClientEvent};
use crate::remote::net::proto::network::PongEvent;
use crate::remote::net::StreamReceiver;
use crate::remote::system::{NodeId, RemoteActorSystem};
use chrono::{DateTime, Utc};
use protobuf::Message as ProtoMessage;

use std::str::FromStr;

use crate::remote::config::SystemCapabilities;
use tokio::sync::oneshot::Sender;
use uuid::Uuid;

pub struct ClientMessageReceiver {
    actor_ref: LocalActorRef<RemoteClient>,
    identity_sender: Option<Sender<NodeIdentity>>,
    should_close: bool,
    addr: String,
}

impl ClientMessageReceiver {
    pub fn new(
        actor_ref: LocalActorRef<RemoteClient>,
        identity_sender: Sender<NodeIdentity>,
        addr: String,
    ) -> ClientMessageReceiver {
        let identity_sender = Some(identity_sender);
        Self {
            actor_ref,
            identity_sender,
            addr,
            should_close: false,
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
                        node: (&identity).into(),
                        peers: identity.peers.into_iter().map(|n| n.into()).collect(),
                        capabilities: identity
                            .capabilities
                            .map(|capabilities| SystemCapabilities {
                                actors: capabilities.actors.to_vec(),
                                messages: capabilities.messages.to_vec(),
                            })
                            .unwrap_or_else(|| SystemCapabilities::default()),
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
                    .map(|n| n.into())
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
                    warn!("error sending handshake_tx");
                }
            }
            ClientEvent::Result(res) => {
                match sys.pop_request(res.message_id) {
                    Some(res_tx) => {
                        let _ = res_tx.send(RemoteResponse::Ok(res.result));
                    }
                    None => {
                        trace!(
                            "node_tag={}, node_id={}, received unknown request result (id={})",
                            sys.node_tag(),
                            sys.node_id(),
                            res.message_id
                        );
                    }
                }
            }
            ClientEvent::Err(e) => {
                debug!("received client error!");
                match sys.pop_request(e.message_id) {
                    Some(res_tx) => {
                        let _ = res_tx.send(RemoteResponse::Err(e.error.unwrap().into()));
                    }
                    None => {
                        //                                          :P
                        warn!("received unsolicited client error");
                    }
                }
            }
            ClientEvent::Ping(_ping) => {}
            ClientEvent::Pong(pong) => {
                match sys.pop_request(pong.message_id) {
                    Some(res_tx) => {
                        let _ = res_tx.send(RemoteResponse::Ok(
                            PongEvent {
                                message_id: pong.message_id,
                                ..Default::default()
                            }
                            .write_to_bytes()
                            .expect("serialised pong"),
                        ));
                    }
                    None => {
                        //                          :P
                        warn!("received unsolicited pong");
                    }
                }
            }
        }
    }

    async fn on_close(&mut self, _sys: &RemoteActorSystem) {
        info!("closed, sending `Disconnected` to {:?}", &self.actor_ref);
        let _ = self.actor_ref.send(Disconnected).await;
    }

    fn on_deserialisation_failed(&mut self) {
        warn!("message serialisation failed (addr={})", &self.addr);
    }

    fn on_stream_lost(&mut self, error: Error) {
        warn!(
            "stream connection lost (addr={}) - error: {}",
            &self.addr, error
        );
    }

    async fn close(&mut self) {
        self.should_close = true;
    }

    fn should_close(&self) -> bool {
        self.should_close
    }
}
