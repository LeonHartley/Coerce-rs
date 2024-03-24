use crate::actor::context::{ActorContext, LogContext};
use crate::actor::message::Handler;
use crate::actor::{Actor, ActorId, IntoActorId, LocalActorRef};
use crate::remote::actor::message::NodeTerminated;
use crate::remote::actor::RemoteResponse;
use crate::remote::cluster::discovery::{Discover, Seed};
use crate::remote::cluster::node::{NodeAttributes, RemoteNode};
use crate::remote::net::message::{
    datetime_to_timestamp, timestamp_to_datetime, ClientEvent, SessionEvent,
};
use crate::remote::net::proto::network::{
    ActorAddress, ClientHandshake, ClientResult, CreateActorEvent, MessageRequest, NodeIdentity,
    PongEvent, RemoteNode as RemoteNodeProto, SessionHandshake, StreamPublishEvent,
    SystemCapabilities,
};
use crate::remote::net::server::session::store::{RemoteSessionStore, SessionClosed, SessionWrite};
use crate::remote::net::server::RemoteServerConfigRef;
use crate::remote::net::{receive_loop, StreamData, StreamReceiver};
use crate::remote::stream::mediator::PublishRaw;
use crate::remote::system::{NodeId, RemoteActorSystem};
use crate::CARGO_PKG_VERSION;
use futures::{SinkExt, StreamExt};
use protobuf::well_known_types::wrappers::UInt64Value;
use protobuf::{Message as ProtoMessage, MessageField};

use bytes::Bytes;
use std::io::Error;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use tokio::io::{ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;
use valuable::Valuable;

pub mod store;

pub struct RemoteSession {
    id: i64,
    addr: SocketAddr,
    write: FramedWrite<WriteHalf<TcpStream>, LengthDelimitedCodec>,
    read: Option<FramedRead<ReadHalf<TcpStream>, LengthDelimitedCodec>>,
    read_cancellation_token: Option<CancellationToken>,
    remote_server_config: RemoteServerConfigRef,
}

impl RemoteSession {
    pub fn new(
        id: i64,
        addr: SocketAddr,
        stream: TcpStream,
        remote_server_config: RemoteServerConfigRef,
    ) -> RemoteSession {
        let (read, write) = tokio::io::split(stream);
        let read = Some(FramedRead::new(read, LengthDelimitedCodec::new()));
        let write = FramedWrite::new(write, LengthDelimitedCodec::new());
        RemoteSession {
            id,
            addr,
            write,
            read,
            read_cancellation_token: Some(CancellationToken::new()),
            remote_server_config,
        }
    }
}

#[async_trait]
impl Actor for RemoteSession {
    async fn started(&mut self, ctx: &mut ActorContext) {
        let log = ctx.log();
        let system = ctx.system().remote_owned();

        debug!(
            ctx = log.as_value(),
            "session started (addr={}, session_id={}), validating token", &self.addr, &self.id
        );

        if let Some(read) = &mut self.read {
            if !validate_session_token(ctx, log, &system, read).await {
                ctx.stop(None);
                return;
            }
        }

        let peers = system
            .get_nodes()
            .await
            .into_iter()
            .map(|node| node.into())
            .collect::<Vec<RemoteNodeProto>>();

        let capabilities = system.config().get_capabilities();
        let capabilities = Some(SystemCapabilities {
            actors: capabilities.actors.into(),
            messages: capabilities.messages.into(),
            ..Default::default()
        });

        self.write(ClientEvent::Identity(NodeIdentity {
            node_id: system.node_id(),
            node_tag: system.node_tag().to_string(),
            application_version: format!("pkg_version={},protocol_version=1", CARGO_PKG_VERSION),
            addr: self.remote_server_config.external_node_addr.to_string(),
            node_started_at: Some(datetime_to_timestamp(system.started_at())).into(),
            peers: peers.into(),
            capabilities: capabilities.into(),
            attributes: system
                .config()
                .get_attributes()
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            ..Default::default()
        }))
        .await;

        let _session = tokio::spawn(receive_loop(
            system.clone(),
            self.read.take().unwrap(),
            SessionMessageReceiver::new(
                self.id,
                self.actor_ref(ctx),
                self.addr,
                self.remote_server_config.clone(),
            ),
        ));
    }

    async fn stopped(&mut self, ctx: &mut ActorContext) {
        debug!(
            "session closed (addr={}, session_id={})",
            &self.addr, &self.id
        );

        let _ = self.write.close().await;

        if let Some(session_store) = ctx.parent::<RemoteSessionStore>() {
            let _ = session_store.send(SessionClosed(self.id)).await;
        }
    }
}

async fn validate_session_token(
    ctx: &mut ActorContext,
    log: LogContext,
    system: &RemoteActorSystem,
    read: &mut FramedRead<ReadHalf<TcpStream>, LengthDelimitedCodec>,
) -> bool {
    let bytes = read.next().await;
    if let Some(Ok(bytes)) = bytes {
        match SessionEvent::read_from_bytes(bytes.to_vec()) {
            Some(SessionEvent::Identify(identify)) => {
                let token = identify.token;
                let token_valid = system
                    .config()
                    .security()
                    .client_authentication()
                    .validate_token(token.as_str());

                if !token_valid {
                    error!(
                        ctx = log.as_value(),
                        "invalid token received, disconnecting session({})",
                        ctx.id(),
                    );
                } else {
                    info!(
                        ctx = log.as_value(),
                        "token validated - connection accepted",
                    );

                    return true;
                }
            }

            value => {
                error!(
                    ctx = log.as_value(),
                    "initial payload invalid, expected SessionEvent::Identify, disconnecting session({}), value={:?}",
                    ctx.id(), value
                );
            }
        }
    } else {
        error!(
            ctx = log.as_value(),
            "unable to read initial authentication payload"
        );
    }

    false
}

#[async_trait]
impl Handler<SessionWrite> for RemoteSession {
    async fn handle(&mut self, message: SessionWrite, _ctx: &mut ActorContext) {
        self.write(message.1).await
    }
}

impl RemoteSession {
    pub async fn write(&mut self, message: ClientEvent) {
        match message.write_to_bytes() {
            Some(msg) => {
                trace!("message encoded");
                if self.write.send(Bytes::from(msg)).await.is_ok() {
                    trace!("message sent");
                } else {
                    error!("failed to send message");
                }
            }
            None => {
                warn!("failed to encode message");
            }
        }
    }
}

pub struct SessionMessageReceiver {
    session_id: i64,
    session: LocalActorRef<RemoteSession>,
    node_id: Option<NodeId>,
    addr: SocketAddr,
    should_close: bool,
    server_config: RemoteServerConfigRef,
}

#[derive(Debug)]
pub enum RemoteSessionErr {
    Encoding,
    StreamErr(tokio::io::Error),
}

impl SessionMessageReceiver {
    pub fn new(
        session_id: i64,
        session: LocalActorRef<RemoteSession>,
        addr: SocketAddr,
        server_config: RemoteServerConfigRef,
    ) -> SessionMessageReceiver {
        SessionMessageReceiver {
            session_id,
            session,
            addr,
            server_config,
            node_id: None,
            should_close: false,
        }
    }
}

#[async_trait]
impl StreamReceiver for SessionMessageReceiver {
    type Message = SessionEvent;

    async fn on_receive(&mut self, msg: SessionEvent, sys: &RemoteActorSystem) {
        match msg {
            SessionEvent::Identify(identify) => {
                let _session_id = self.session_id;
                let _system = sys.clone();
                let _session = self.session.clone();

                trace!(
                    "received identify from node (id={}, tag={}), session_id={}",
                    &identify.source_node_id,
                    &identify.source_node_tag,
                    &self.session_id
                );
            }

            SessionEvent::Handshake(msg) => {
                trace!(
                    "handshake {}, {:?}, type: {:?}",
                    &msg.node_id,
                    &msg.nodes,
                    &msg.client_type
                );

                tokio::spawn(session_handshake(
                    sys.clone(),
                    msg,
                    self.session_id,
                    self.session.clone(),
                    self.addr,
                    self.server_config.clone(),
                ));
            }

            SessionEvent::FindActor(find_actor) => {
                trace!(
                    "actor lookup {}, {}",
                    &self.session_id,
                    &find_actor.actor_id
                );
                tokio::spawn(session_handle_lookup(
                    find_actor.message_id,
                    find_actor.actor_id.into_actor_id(),
                    self.session_id,
                    sys.clone(),
                    self.session.clone(),
                ));
            }

            SessionEvent::RegisterActor(actor) => {
                if let Some(node_id) = actor.node_id.into_option() {
                    trace!("register actor {}, {}", &actor.actor_id, &node_id.value);
                    let node_id = node_id.value;
                    sys.register_actor(actor.actor_id.into_actor_id(), Some(node_id));
                }
            }

            SessionEvent::NotifyActor(msg) => {
                tokio::spawn(session_handle_message(
                    msg,
                    self.session_id,
                    sys.clone(),
                    self.session.clone(),
                ));
            }

            SessionEvent::Ping(ping) => {
                if sys.actor_system().is_terminated() {
                    warn!("Ping received but system is terminated. Closing connection (session_id={})", self.session_id);

                    let _ = self.session.notify_stop();
                    self.close().await;
                    return;
                }

                if ping.system_terminated {
                    let _ = sys.heartbeat().notify(NodeTerminated(ping.node_id));

                    debug!(
                        "Notified registry - node_id={} is terminated (session_id={})",
                        &ping.node_id, &self.session_id
                    );
                    self.close();
                } else {
                    trace!("ping received, sending pong");
                    self.session
                        .send(SessionWrite(
                            self.session_id,
                            ClientEvent::Pong(PongEvent {
                                message_id: ping.message_id,
                                ..Default::default()
                            }),
                        ))
                        .await
                        .expect("send session write");
                }
            }

            SessionEvent::Pong(_id) => {}

            SessionEvent::CreateActor(msg) => {
                trace!("create actor {}, {:?}", self.session_id, &msg.actor_id);
                tokio::spawn(session_create_actor(
                    msg,
                    self.session_id,
                    sys.clone(),
                    self.session.clone(),
                ));
            }

            SessionEvent::StreamPublish(msg) => {
                trace!("stream publish {}, {:?}", self.session_id, &msg);
                tokio::spawn(session_stream_publish(msg, sys.clone()));
            }

            SessionEvent::Raft(_req) => {}

            SessionEvent::Result(res) => match sys.pop_request(res.message_id) {
                Some(res_tx) => {
                    let _ = res_tx.send(RemoteResponse::Ok(res.result));
                }
                None => {
                    warn!(
                        "node_tag={}, node_id={}, received unknown request result (id={})",
                        sys.node_tag(),
                        sys.node_id(),
                        res.message_id
                    );
                }
            },
            SessionEvent::Err(err) => {
                let e = err.error.unwrap().into();
                match sys.pop_request(err.message_id) {
                    Some(res_tx) => {
                        let _ = res_tx.send(RemoteResponse::Err(e));
                    }
                    None => {
                        warn!(
                            "node_tag={}, node_id={}, received unknown request err (id={})",
                            sys.node_tag(),
                            sys.node_id(),
                            e
                        );
                    }
                }
            }
        }
    }

    async fn on_close(&mut self, _ctx: &RemoteActorSystem) {
        let _ = self.session.notify_stop();
    }

    fn on_deserialisation_failed(&mut self) {
        warn!(
            "message serialisation failed (addr={}, session_id={})",
            &self.addr, &self.session_id
        )
    }

    fn on_stream_lost(&mut self, error: Error) {
        warn!(
            "stream connection lost (addr={}, session_id={}) - error: {}",
            &self.addr, self.session_id, error
        );
    }

    async fn close(&mut self) {
        self.should_close = true;
    }

    fn should_close(&self) -> bool {
        self.should_close
    }
}

async fn session_handshake(
    ctx: RemoteActorSystem,
    handshake: SessionHandshake,
    session_id: i64,
    session: LocalActorRef<RemoteSession>,
    session_addr: SocketAddr,
    server_config: RemoteServerConfigRef,
) {
    // let mut headers = HashMap::<String, String>::new();
    // headers.insert("traceparent".to_owned(), handshake.trace_id);
    // let span = tracing::trace_span!("RemoteServer::Handshake");
    // span.set_parent(global::get_text_map_propagator(|propagator| {
    //     propagator.extract(&mut headers)
    // }));
    // let _enter = span.enter();

    debug!(
        "received SessionHandshake (request_id={})",
        &handshake.trace_id
    );

    let nodes = ctx.get_nodes().await;
    let response = ClientHandshake {
        node_id: ctx.node_id(),
        node_tag: ctx.node_tag().to_string(),
        node_started_at: Some(datetime_to_timestamp(ctx.started_at())).into(),
        trace_id: handshake.trace_id.clone(),
        nodes: nodes.into_iter().map(|n| n.into()).collect(),
        ..ClientHandshake::default()
    };

    let self_id = ctx.node_id();

    let nodes = handshake
        .nodes
        .into_iter()
        .filter(|n| n.node_id != self_id)
        .map(|n| {
            let started_at = n.node_started_at.into_option().map(timestamp_to_datetime);

            let addr =
                if server_config.override_incoming_node_addr && n.node_id == handshake.node_id {
                    let host = session_addr.ip();
                    info!("parsing addr={}", &n.addr);
                    let port = n.addr.split(":").last().unwrap();
                    format!("{}:{}", host, port)
                } else {
                    n.addr
                };

            let attributes: NodeAttributes = n
                .attributes
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect();
            RemoteNode::new(n.node_id, addr, n.tag, started_at, attributes.into())
        })
        .collect();

    let sys = ctx.clone();

    info!(
        "[{}] discovering nodes: {:?}, request_id={}",
        &session_id, &nodes, &handshake.trace_id
    );

    let (tx, rx) = oneshot::channel();
    let _ = sys.node_discovery().notify(Discover {
        seed: Seed::Nodes(nodes),
        on_discovery_complete: Some(tx),
    });

    let _ = rx.await.expect("unable to discover nodes");

    info!(
        "[{}] discovered nodes, request_id={}",
        &session_id, &handshake.trace_id
    );

    session
        .send(SessionWrite(session_id, ClientEvent::Handshake(response)))
        .await
        .expect("send session write (handshake)");

    info!(
        "[{}] written handshake ack, request_id={}",
        &session_id, &handshake.trace_id
    );
}

async fn session_handle_message(
    msg: MessageRequest,
    session_id: i64,
    ctx: RemoteActorSystem,
    session: LocalActorRef<RemoteSession>,
) {
    // let mut headers = HashMap::<String, String>::new();
    // headers.insert("traceparent".to_owned(), msg.trace_id);
    // let span = tracing::trace_span!("RemoteServer::MessageRequest");
    // span.set_parent(global::get_text_map_propagator(|propagator| {
    //     propagator.extract(&mut headers)
    // }));
    //
    // let _enter = span.enter();

    let actor_id = msg.actor_id.into_actor_id();

    match ctx
        .handle_message(
            msg.handler_type.as_str(),
            actor_id.clone(),
            msg.message.as_slice(),
            msg.requires_response,
        )
        .await
    {
        Ok(buf) => {
            if let Some(buf) = buf {
                send_result(msg.message_id, buf, session_id, session).await;
            }
        }
        Err(e) => {
            error!("[node={}] failed to handle message (handler_type={}, target_actor_id={}), error={:?}", ctx.node_id(), &msg.handler_type, &actor_id, e);
            let _ = ctx
                .notify_rpc_err(msg.message_id, e, msg.origin_node_id)
                .await;
        }
    }
}

async fn session_handle_lookup(
    msg_id: u64,
    id: ActorId,
    session_id: i64,
    ctx: RemoteActorSystem,
    session: LocalActorRef<RemoteSession>,
) {
    let node_id = ctx.locate_actor_node(id.clone()).await;
    trace!("sending actor lookup result: {:?}", node_id);

    let response = ActorAddress {
        actor_id: id.to_string(),
        node_id: MessageField::from_option(node_id.map(|n| UInt64Value::from(n))),
        ..ActorAddress::default()
    };

    match response.write_to_bytes() {
        Ok(buf) => send_result(msg_id, buf, session_id, session).await,
        Err(_) => {
            error!("failed to handle message, todo: send err");
        }
    }
}

async fn session_create_actor(
    msg: CreateActorEvent,
    session_id: i64,
    ctx: RemoteActorSystem,
    session: LocalActorRef<RemoteSession>,
) {
    let msg_id = msg.message_id.clone();
    let actor_id = if msg.actor_id.is_empty() {
        None
    } else {
        Some(msg.actor_id.into_actor_id())
    };

    trace!(
        "node_tag={}, node_id={}, message_id={}, received request to create actor (actor_id={})",
        ctx.node_tag(),
        ctx.node_id(),
        &msg.message_id,
        actor_id.as_ref().map_or_else(|| "N/A", |s| s)
    );
    match ctx
        .handle_create_actor(actor_id, msg.actor_type, msg.recipe)
        .await
    {
        Ok(buf) => send_result(msg_id, buf.to_vec(), session_id, session).await,
        Err(_) => {
            error!("failed to handle message, todo: send err");
        }
    }
}

async fn session_stream_publish(msg: Arc<StreamPublishEvent>, sys: RemoteActorSystem) {
    // TODO: node should acknowledge the message
    if let Some(mediator) = sys.stream_mediator() {
        mediator.notify::<PublishRaw>(msg.into()).unwrap()
    }
}

async fn send_result(
    message_id: u64,
    result: Vec<u8>,
    session_id: i64,
    session: LocalActorRef<RemoteSession>,
) {
    trace!("sending result");

    let event = ClientEvent::Result(ClientResult {
        message_id,
        result,
        ..ClientResult::default()
    });

    if session.send(SessionWrite(session_id, event)).await.is_ok() {
        trace!("sent result successfully");
    } else {
        error!("failed to send result");
    }
}
