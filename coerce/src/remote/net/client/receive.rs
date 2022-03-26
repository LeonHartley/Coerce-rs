use crate::actor::scheduler::timer::Timer;
use crate::actor::LocalActorRef;
use crate::remote::actor::RemoteResponse;
use crate::remote::net::client::connect::Disconnected;
use crate::remote::net::client::RemoteClient;
use crate::remote::net::message::{timestamp_to_datetime, ClientEvent};
use crate::remote::net::proto::network::Pong;
use crate::remote::net::StreamReceiver;
use crate::remote::system::{NodeId, RemoteActorSystem};
use chrono::{DateTime, NaiveDateTime, Utc};
use protobuf::Message;
use std::str::FromStr;
use tokio::sync::oneshot;
use uuid::Uuid;

pub struct ClientMessageReceiver {
    node_id: Option<NodeId>,
    handshake_tx: Option<oneshot::Sender<HandshakeAcknowledge>>,
    actor_ref: LocalActorRef<RemoteClient>,
}

impl ClientMessageReceiver {
    pub fn new(
        node_id: Option<NodeId>,
        handshake_tx: oneshot::Sender<HandshakeAcknowledge>,
        actor_ref: LocalActorRef<RemoteClient>,
    ) -> ClientMessageReceiver {
        let handshake_tx = Some(handshake_tx);
        Self {
            node_id,
            handshake_tx,
            actor_ref,
        }
    }
}

pub struct HandshakeAcknowledge {
    pub node_id: NodeId,
    pub node_tag: String,
    pub node_started_at: DateTime<Utc>,
}

#[async_trait]
impl StreamReceiver for ClientMessageReceiver {
    type Message = ClientEvent;

    async fn on_receive(&mut self, msg: ClientEvent, sys: &RemoteActorSystem) {
        match msg {
            ClientEvent::Handshake(msg) => {
                let node_id = msg.node_id;

                let nodes = msg
                    .nodes
                    .into_iter()
                    .filter(|n| n.node_id != node_id)
                    .map(|n| crate::remote::cluster::node::RemoteNode {
                        id: n.get_node_id(),
                        addr: n.addr,
                        tag: n.tag,
                        node_started_at: n.node_started_at.into_option().map(timestamp_to_datetime),
                    })
                    .collect();

                sys.notify_register_nodes(nodes);

                self.node_id = Some(node_id);

                if let Some(handshake_tx) = self.handshake_tx.take() {
                    let node_tag = msg.node_tag;
                    let node_started_at = msg
                        .node_started_at
                        .into_option()
                        .map_or_else(|| Utc::now(), timestamp_to_datetime);

                    if !handshake_tx
                        .send(HandshakeAcknowledge {
                            node_id,
                            node_tag,
                            node_started_at,
                        })
                        .is_ok()
                    {
                        warn!(target: "RemoteClient", "error sending handshake_tx");
                    }
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

    async fn on_close(&mut self, sys: &RemoteActorSystem) {
        let _ = self.actor_ref.send(Disconnected).await;
    }
}
