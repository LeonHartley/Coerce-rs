use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message};
use crate::actor::{Actor, IntoActor, LocalActorRef};
use crate::remote::cluster::node::{RemoteNode, RemoteNodeState};
use crate::remote::net::client::receive::ClientMessageReceiver;
use crate::remote::net::client::{
    write_bytes, ClientState, ClientType, ConnectionState, RemoteClient,
};
use crate::remote::net::codec::NetworkCodec;
use crate::remote::net::message::{datetime_to_timestamp, SessionEvent};
use crate::remote::net::proto::network as proto;
use crate::remote::net::{receive_loop, StreamData};
use crate::remote::system::{NodeId, RemoteActorSystem};
use crate::remote::tracing::extract_trace_identifier;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio_util::codec::{FramedRead, FramedWrite};

pub struct Connect {
    pub outbound_seed_nodes: Option<Vec<RemoteNodeState>>,
}

impl Connect {
    pub fn new(outbound_seed_nodes: Option<Vec<RemoteNodeState>>) -> Self {
        Self {
            outbound_seed_nodes,
        }
    }
}

pub struct Disconnected;

const RECONNECT_DELAY: Duration = Duration::from_millis(1000);

#[async_trait]
impl Handler<Connect> for RemoteClient {
    async fn handle(&mut self, message: Connect, ctx: &mut ActorContext) -> bool {
        if let Some(connection_state) = self.connect(message, ctx).await {
            let remote = ctx.system().remote();
            let node_id = connection_state.node_id;

            remote.notify_register_node(RemoteNode::new(
                connection_state.node_id,
                self.addr.clone(),
                connection_state.node_tag.clone(),
                Some(connection_state.node_started_at),
            ));

            remote.register_client(node_id, self.actor_ref(ctx)).await;
            self.state = Some(ClientState::Connected(connection_state));

            debug!(
                "RemoteClient connected to node (addr={}, node_id={})",
                &self.addr, node_id
            );

            self.flush_buffered_writes().await;

            true
        } else {
            warn!("RemoteClient failed to connect");
            self.handle(Disconnected, ctx).await;
            false
        }
    }
}

#[async_trait]
impl Handler<Disconnected> for RemoteClient {
    async fn handle(&mut self, _msg: Disconnected, ctx: &mut ActorContext) {
        // TODO: try to connect again, if fails after {n} attempts with a timeout,
        //       we should quarantine the node and ensuring the node no longer
        //       participates in cluster activities/sharding

        warn!(
            "RemoteClient connection to node (addr={}) closed/failed, retrying in {}ms",
            &self.addr,
            RECONNECT_DELAY.as_millis()
        );

        let state = match self.state.take().unwrap() {
            ClientState::Idle {
                connection_attempts,
            } => {
                let connection_attempts = connection_attempts + 1;

                ClientState::Idle {
                    connection_attempts,
                }
            }

            ClientState::Quarantined {
                since,
                connection_attempts,
            } => {
                let connection_attempts = connection_attempts + 1;

                ClientState::Quarantined {
                    since,
                    connection_attempts,
                }
            }

            ClientState::Connected(mut state) => {
                state.disconnected().await;

                ClientState::Idle {
                    connection_attempts: 1,
                }
            }
        };

        self.state = Some(state);

        let self_ref = self.actor_ref(ctx);
        tokio::spawn(async move {
            tokio::time::sleep(RECONNECT_DELAY).await;
            let _res = self_ref.send(Connect::new(None)).await;
        });
    }
}

impl Message for Connect {
    type Result = bool;
}

impl Message for Disconnected {
    type Result = ();
}

impl RemoteClient {
    pub async fn connect(
        &mut self,
        connect: Connect,
        ctx: &mut ActorContext,
    ) -> Option<ConnectionState> {
        let span = tracing::trace_span!("RemoteClient::connect", address = self.addr.as_str());

        let _enter = span.enter();
        let stream = TcpStream::connect(&self.addr).await;
        if stream.is_err() {
            return None;
        }

        let stream = stream.unwrap();
        let (read, writer) = tokio::io::split(stream);

        let reader = FramedRead::new(read, NetworkCodec);
        let mut writer = FramedWrite::new(writer, NetworkCodec);

        let (handshake_tx, handshake_rx) = oneshot::channel();

        let remote = ctx.system().remote_owned();
        let node_id = remote.node_id();
        let node_tag = remote.node_tag().to_string();

        let receive_task = tokio::spawn(receive_loop(
            remote.clone(),
            reader,
            ClientMessageReceiver::new(
                self.remote_node_id.clone(),
                handshake_tx,
                self.actor_ref(ctx),
            ),
        ));

        trace!("writing handshake");

        let trace_id = extract_trace_identifier(&span);

        let seed_nodes = match connect.outbound_seed_nodes {
            Some(seed_nodes) => seed_nodes.into_iter(),
            None => remote.get_nodes().await.into_iter(),
        };

        write_bytes(
            SessionEvent::Handshake(proto::SessionHandshake {
                node_id,
                node_tag,
                token: vec![],
                client_type: self.client_type.into(),
                trace_id,
                nodes: seed_nodes
                    .map(|node| proto::RemoteNode {
                        node_id: node.id,
                        addr: node.addr,
                        tag: node.tag,
                        node_started_at: node
                            .node_started_at
                            .as_ref()
                            .map(datetime_to_timestamp)
                            .into(),
                        ..proto::RemoteNode::default()
                    })
                    .collect(),
                ..proto::SessionHandshake::default()
            })
            .write_to_bytes()
            .unwrap()
            .as_ref(),
            &mut writer,
        )
        .await
        .expect("write handshake");

        trace!("waiting for handshake ack");
        let handshake_ack = handshake_rx.await.expect("handshake ack");
        let node_id = handshake_ack.node_id;
        let node_tag = handshake_ack.node_tag;
        let node_started_at = handshake_ack.node_started_at;
        let write = writer;
        Some(ConnectionState {
            node_id,
            node_tag,
            node_started_at,
            write,
            receive_task,
        })
    }
}
