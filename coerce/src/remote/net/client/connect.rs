use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message};
use crate::actor::scheduler::timer::Timer;
use crate::actor::{Actor, CoreActorRef, LocalActorRef};
use crate::remote::actor::message::{ClientConnected, RemoveClient};
use crate::remote::cluster::discovery::{Discover, Seed};
use crate::remote::cluster::node::RemoteNode;
use crate::remote::net::client::ping::PingTick;
use crate::remote::net::client::receive::{ClientMessageReceiver, HandshakeAcknowledge};
use crate::remote::net::client::send::write_bytes;
use crate::remote::net::client::{
    BeginHandshake, ClientState, ConnectionState, HandshakeAckCallback, HandshakeStatus,
    RemoteClient,
};
use crate::remote::net::message::SessionEvent;
use crate::remote::net::proto::network::{self as proto, IdentifyEvent};
use crate::remote::net::{receive_loop, StreamData};

use bytes::Bytes;
use chrono::Utc;
use protobuf::EnumOrUnknown;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::sync::oneshot::Sender;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use valuable::Valuable;

pub struct Connect;

pub struct OnConnect(pub Sender<(LocalActorRef<RemoteClient>, RemoteNode)>);

impl RemoteClient {
    pub async fn connect(
        &mut self,
        _connect: Connect,
        ctx: &mut ActorContext,
    ) -> Option<ConnectionState> {
        let log_ctx = ctx.log();
        let stream = TcpStream::connect(&self.addr).await;
        if stream.is_err() {
            let error = stream.unwrap_err();
            error!(
                ctx = log_ctx.as_value(),
                "connection to {} failed, error: {}", &self.addr, error
            );
            return None;
        }

        let stream = stream.unwrap();
        let (read, writer) = tokio::io::split(stream);

        let codec = LengthDelimitedCodec::new();
        let reader = FramedRead::new(read, codec.clone());
        let mut write = FramedWrite::new(writer, codec.clone());

        let (identity_tx, identity_rx) = oneshot::channel();

        let remote = ctx.system().remote_owned();

        let identify = SessionEvent::Identify(IdentifyEvent {
            source_node_id: remote.node_id(),
            source_node_tag: remote.node_tag().to_string(),
            token: remote
                .config()
                .security()
                .client_authentication()
                .generate_token(),
            ..Default::default()
        });

        match write_bytes(Bytes::from(identify.write_to_bytes().unwrap()), &mut write).await {
            Ok(_) => {}
            Err(e) => {
                error!(
                    ctx = log_ctx.as_value(),
                    "failed to write identify message to begin authentication, error={}", e
                );
                return None;
            }
        };

        // TODO: read a token ACK before we proceed

        let receive_task = tokio::spawn(receive_loop(
            remote.clone(),
            reader,
            ClientMessageReceiver::new(self.actor_ref(ctx), identity_tx, self.addr.clone()),
        ));

        self.ping_timer = Some(Timer::start_immediately(
            self.actor_ref(ctx),
            ctx.system().remote().config().heartbeat_config().interval,
            PingTick,
        ));

        let identity = match identity_rx.await {
            Ok(identity) => identity,
            Err(_) => {
                warn!(
                    ctx = log_ctx.as_value(),
                    "no identity received (addr={})", &self.addr
                );
                return None;
            }
        };

        Some(ConnectionState {
            identity,
            handshake: HandshakeStatus::None,
            write,
            receive_task,
        })
    }
}

pub struct Disconnected;

const RECONNECT_DELAY: Duration = Duration::from_millis(5000);

#[async_trait]
impl Handler<Connect> for RemoteClient {
    async fn handle(&mut self, message: Connect, ctx: &mut ActorContext) {
        // let span = tracing::trace_span!("RemoteClient::connect", actor_id = ctx.id().as_str(),);
        //
        // let _enter = span.enter();

        if let Some(state) = &self.state {
            if state.is_connected() {
                return;
            }
        }

        if let Some(connection_state) = self.connect(message, ctx).await {
            let client_actor_ref = self.actor_ref(ctx);
            let _ = ctx
                .system()
                .remote()
                .client_registry()
                .send(ClientConnected {
                    addr: connection_state.identity.node.addr.clone(),
                    remote_node_id: connection_state.identity.node.id,
                    client_actor_ref,
                })
                .await;

            while let Some(callback) = self.on_identified_callbacks.pop() {
                let _ = callback.send(Some(connection_state.identity.clone()));
            }

            self.node_id = Some(connection_state.identity.node.id);
            self.state = Some(ClientState::Connected(connection_state));

            debug!("RemoteClient connected to node (addr={})", &self.addr);

            let _ = ctx.system().remote().node_discovery().notify(Discover {
                seed: Seed::Addr(self.addr.clone()),
                on_discovery_complete: None,
            });

            self.flush_buffered_writes().await;
        } else {
            while let Some(callback) = self.on_identified_callbacks.pop() {
                let _ = callback.send(None);
            }

            self.handle(Disconnected, ctx).await;
        }
    }
}

#[async_trait]
impl Handler<BeginHandshake> for RemoteClient {
    async fn handle(&mut self, message: BeginHandshake, ctx: &mut ActorContext) {
        let mut connection = match &mut self.state {
            Some(ClientState::Connected(connection)) => connection,
            _ => {
                // let actor_ref = self.actor_ref(ctx);
                // let _ = actor_ref.notify(Connect);
                // let _ = actor_ref.notify(message);
                return;
            }
        };

        match &connection.handshake {
            &HandshakeStatus::Acknowledged(_) => {
                let _ = message.on_handshake_complete.send(());
            }

            &HandshakeStatus::Pending => {
                self.on_handshake_ack_callbacks.push(HandshakeAckCallback {
                    request_id: message.request_id,
                    callback: message.on_handshake_complete,
                });
            }

            _ => {
                let remote = ctx.system().remote_owned();
                let node_id = remote.node_id();
                let node_tag = remote.node_tag().to_string();

                connection.handshake = HandshakeStatus::Pending;

                debug!(
                    "writing client handshake (client_addr={}, request_id={})",
                    &self.addr, &message.request_id
                );

                write_bytes(
                    Bytes::from(
                        SessionEvent::Handshake(proto::SessionHandshake {
                            node_id,
                            node_tag,
                            token: vec![],
                            client_type: EnumOrUnknown::new(self.client_type.into()),
                            trace_id: message.request_id.to_string(),
                            nodes: message
                                .seed_nodes
                                .into_iter()
                                .map(|node| node.into())
                                .collect(),
                            ..proto::SessionHandshake::default()
                        })
                        .write_to_bytes()
                        .unwrap(),
                    ),
                    &mut connection.write,
                )
                .await
                .expect("write handshake");

                debug!(
                    "written client handshake (client_addr={}, request_id={})",
                    &self.addr, &message.request_id
                );

                self.on_handshake_ack_callbacks.push(HandshakeAckCallback {
                    request_id: message.request_id,
                    callback: message.on_handshake_complete,
                });
            }
        }
    }
}

#[async_trait]
impl Handler<HandshakeAcknowledge> for RemoteClient {
    async fn handle(&mut self, message: HandshakeAcknowledge, _ctx: &mut ActorContext) {
        info!(
            "handshake acknowledged (addr={}, node_id={}, node_tag={})",
            &self.addr, &message.node_id, &message.node_tag
        );

        match &mut self.state {
            Some(ClientState::Connected(state)) => {
                state.handshake = HandshakeStatus::Acknowledged(message);

                while let Some(callback) = self.on_handshake_ack_callbacks.pop() {
                    debug!(
                        "ack callback executed (request_id={}, client_addr={})",
                        callback.request_id, &self.addr
                    );

                    let _ = callback.callback.send(());
                }
            }
            _ => {
                warn!("received HandshakeAck but the client connection state is invalid, addr={}, node_id={}", &self.addr, message.node_id);
            }
        }
    }
}

#[async_trait]
impl Handler<Disconnected> for RemoteClient {
    async fn handle(&mut self, _msg: Disconnected, ctx: &mut ActorContext) {
        if let Some(true) = self.state.as_ref().map(|n| n.is_connected()) {
            warn!(
                addr = &self.addr,
                reconnect_delay_millis = RECONNECT_DELAY.as_millis(),
                "RemoteClient disconnected from node",
            );
        } else {
            warn!(
                addr = &self.addr,
                reconnect_delay_millis = RECONNECT_DELAY.as_millis(),
                "RemoteClient failed to re-connect to node",
            );
        }

        let mut reconnect = true;
        let state = match self.state.take().unwrap() {
            ClientState::Idle {
                connection_attempts,
            } => {
                let connection_attempts = connection_attempts + 1;
                if connection_attempts > 10 {
                    reconnect = false;

                    warn!(
                        addr = &self.addr,
                        connection_attempts = connection_attempts,
                        "client terminating, no longer attempting to re-connect"
                    );

                    ClientState::Terminated
                } else {
                    ClientState::Idle {
                        connection_attempts,
                    }
                }
            }

            ClientState::Connected(mut state) => ClientState::Idle {
                connection_attempts: 1,
            },

            state => state,
        };

        self.state = Some(state);

        if reconnect {
            let self_ref = self.actor_ref(ctx);
            tokio::spawn(async move {
                tokio::time::sleep(RECONNECT_DELAY).await;
                let _res = self_ref.send(Connect).await;
            });
        } else {
            let _ = ctx
                .system()
                .remote()
                .client_registry()
                .send(RemoveClient {
                    addr: self.addr.clone(),
                    node_id: self.node_id,
                })
                .await;

            let _ = ctx.boxed_actor_ref().notify_stop();
        }
    }
}

impl Message for Connect {
    type Result = ();
}

impl Message for Disconnected {
    type Result = ();
}
