use crate::remote::system::RemoteActorSystem;

use crate::actor::{ActorId, LocalActorRef};
use crate::remote::net::codec::NetworkCodec;
use crate::remote::net::message::{ClientEvent, SessionEvent};
use crate::remote::net::server::session::{
    NewSession, RemoteSession, RemoteSessionStore, SessionClosed, SessionWrite,
};
use crate::remote::net::{receive_loop, StreamReceiver};

use crate::actor::scheduler::ActorType::{Anonymous, Tracked};
use crate::remote::cluster::node::RemoteNode;
use crate::remote::net::proto::protocol::{
    ActorAddress, ClientErr, ClientHandshake, ClientResult, CreateActor, MessageRequest, Pong,
    RemoteNode as RemoteNodeProto, SessionHandshake, StreamPublish,
};
use crate::remote::stream::mediator::PublishRaw;
use opentelemetry::global;
use protobuf::Message;
use slog::Logger;
use std::collections::HashMap;
use std::str::FromStr;
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;

pub mod session;

pub struct RemoteServer {
    stop: Option<tokio::sync::oneshot::Sender<bool>>,
}

pub struct SessionMessageReceiver {
    session_id: Uuid,
    sessions: LocalActorRef<RemoteSessionStore>,
    log: Logger,
}

#[derive(Debug)]
pub enum RemoteSessionErr {
    Encoding,
    StreamErr(tokio::io::Error),
}

#[derive(Debug)]
pub enum RemoteServerErr {
    Startup,
    StreamErr(tokio::io::Error),
}

impl SessionMessageReceiver {
    pub fn new(
        session_id: Uuid,
        sessions: LocalActorRef<RemoteSessionStore>,
        log: Logger,
    ) -> SessionMessageReceiver {
        SessionMessageReceiver {
            session_id,
            sessions,
            log,
        }
    }
}

impl RemoteServer {
    pub fn new() -> Self {
        RemoteServer { stop: None }
    }

    pub async fn start(
        &mut self,
        addr: String,
        system: RemoteActorSystem,
    ) -> Result<(), tokio::io::Error> {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        let (stop_tx, _stop_rx) = tokio::sync::oneshot::channel();

        let session_store = system
            .actor_system()
            .new_actor(
                format!("RemoteSessionStore-{}", system.node_tag()),
                RemoteSessionStore::new(),
                Anonymous,
            )
            .await
            .unwrap();

        tokio::spawn(server_loop(listener, system, session_store));
        self.stop = Some(stop_tx);
        Ok(())
    }

    pub fn stop(&mut self) -> bool {
        if let Some(stop) = self.stop.take() {
            stop.send(true).is_ok()
        } else {
            false
        }
    }
}

pub async fn server_loop(
    listener: tokio::net::TcpListener,
    system: RemoteActorSystem,
    session_store: LocalActorRef<RemoteSessionStore>,
) {
    let log = system.actor_system().log().new(o!(
        "context" => "RemoteServer"
    ));

    loop {
        match listener.accept().await {
            Ok((socket, addr)) => {
                trace!(&log, "client accepted {}", addr);

                let (read, write) = tokio::io::split(socket);

                let read = FramedRead::new(read, NetworkCodec);
                let write = FramedWrite::new(write, NetworkCodec);
                let (_stop_tx, stop_rx) = tokio::sync::oneshot::channel();

                let session_id = Uuid::new_v4();

                session_store
                    .send(NewSession(RemoteSession::new(session_id, write)))
                    .await
                    .expect("new session registered");

                let log = log.new(o!(
                    "context" => "ServerSession",
                    "session-id" => session_id.to_string()
                ));

                let _session = tokio::spawn(receive_loop(
                    system.clone(),
                    read,
                    stop_rx,
                    SessionMessageReceiver::new(session_id, session_store.clone(), log.clone()),
                    log,
                ));
            }
            Err(e) => error!(&log, "error accepting client: {:?}", e),
        }
    }
}

#[async_trait]
impl StreamReceiver for SessionMessageReceiver {
    type Message = SessionEvent;

    async fn on_receive(&mut self, msg: SessionEvent, ctx: &RemoteActorSystem) {
        match msg {
            SessionEvent::Handshake(msg) => {
                trace!(
                    &self.log,
                    "handshake {}, {:?}, type: {:?}",
                    &msg.node_id,
                    &msg.nodes,
                    &msg.client_type
                );
                tokio::spawn(session_handshake(
                    ctx.clone(),
                    msg,
                    self.session_id,
                    self.sessions.clone(),
                ));
            }

            SessionEvent::FindActor(find_actor) => {
                trace!(
                    &self.log,
                    "actor lookup {}, {}",
                    &self.session_id,
                    &find_actor.actor_id
                );
                tokio::spawn(session_handle_lookup(
                    Uuid::from_str(&find_actor.message_id).unwrap(),
                    find_actor.actor_id,
                    self.session_id,
                    ctx.clone(),
                    self.sessions.clone(),
                    self.log.clone(),
                ));
            }

            SessionEvent::RegisterActor(actor) => {
                let node_id = Uuid::from_str(actor.get_node_id()).unwrap();

                trace!(
                    &self.log,
                    "register actor {}, {}",
                    &actor.actor_id,
                    &actor.node_id
                );
                ctx.register_actor(actor.actor_id, Some(node_id));
            }

            SessionEvent::NotifyActor(msg) => {
                tokio::spawn(session_handle_message(
                    msg,
                    self.session_id,
                    ctx.clone(),
                    self.sessions.clone(),
                    self.log.clone(),
                ));
            }

            SessionEvent::Ping(ping) => {
                trace!(&self.log, "ping received, sending pong");
                self.sessions
                    .send(SessionWrite(
                        self.session_id,
                        ClientEvent::Pong(Pong {
                            message_id: ping.message_id,
                            ..Pong::default()
                        }),
                    ))
                    .await
                    .expect("send session write");
            }

            SessionEvent::Pong(_id) => {}

            SessionEvent::CreateActor(msg) => {
                trace!(
                    &self.log,
                    "create actor {}, {:?}",
                    self.session_id,
                    &msg.actor_id
                );
                tokio::spawn(session_create_actor(
                    msg,
                    self.session_id,
                    ctx.clone(),
                    self.sessions.clone(),
                    self.log.clone(),
                ));
            }

            SessionEvent::StreamPublish(msg) => {
                trace!(&self.log, "stream publish {}, {:?}", self.session_id, &msg);
                tokio::spawn(session_stream_publish(msg, ctx.clone(), self.log.clone()));
            }
        }
    }

    async fn on_close(&mut self, _ctx: &RemoteActorSystem) {
        self.sessions
            .send(SessionClosed(self.session_id))
            .await
            .expect("send session closed")
    }
}

async fn session_handshake(
    ctx: RemoteActorSystem,
    handshake: SessionHandshake,
    session_id: Uuid,
    sessions: LocalActorRef<RemoteSessionStore>,
) {
    let mut headers = HashMap::<String, String>::new();
    headers.insert("traceparent".to_owned(), handshake.trace_id);
    let span = tracing::trace_span!("RemoteServer::Handshake");
    span.set_parent(global::get_text_map_propagator(|propagator| {
        propagator.extract(&mut headers)
    }));

    let _enter = span.enter();

    let nodes = ctx.get_nodes().await;
    let mut response = ClientHandshake {
        node_id: ctx.node_id().to_string(),
        node_tag: ctx.node_tag().to_string(),
        ..ClientHandshake::default()
    };

    for node in nodes {
        response.nodes.push(RemoteNodeProto {
            node_id: node.id.to_string(),
            addr: node.addr,
            ..RemoteNodeProto::default()
        });
    }

    sessions
        .send(SessionWrite(session_id, ClientEvent::Handshake(response)))
        .await
        .expect("send session write (handshake)");

    ctx.register_nodes(
        handshake
            .nodes
            .into_iter()
            .map(|n| RemoteNode::new(n.node_id.parse().unwrap(), n.addr))
            .collect(),
    )
    .await;
}

async fn session_handle_message(
    msg: MessageRequest,
    session_id: Uuid,
    ctx: RemoteActorSystem,
    mut sessions: LocalActorRef<RemoteSessionStore>,
    log: Logger,
) {
    let mut headers = HashMap::<String, String>::new();
    headers.insert("traceparent".to_owned(), msg.trace_id);
    let span = tracing::trace_span!("RemoteServer::MessageRequest");
    span.set_parent(global::get_text_map_propagator(|propagator| {
        propagator.extract(&mut headers)
    }));

    let _enter = span.enter();

    match ctx
        .handle_message(msg.handler_type, msg.actor_id, msg.message.as_slice())
        .await
    {
        Ok(buf) => {
            send_result(
                msg.message_id.parse().unwrap(),
                buf,
                session_id,
                &mut sessions,
                &log,
            )
            .await
        }
        Err(_) => {
            error!(&log, "failed to handle message, todo: send err");
        }
    }
}

async fn session_handle_lookup(
    msg_id: Uuid,
    id: ActorId,
    session_id: Uuid,
    ctx: RemoteActorSystem,
    sessions: LocalActorRef<RemoteSessionStore>,
    log: Logger,
) {
    let node_id = ctx.locate_actor_node(id.clone()).await;
    trace!(&log, "sending actor lookup result: {:?}", node_id);

    let response = ActorAddress {
        actor_id: id,
        node_id: node_id.map_or_else(|| String::default(), |n| n.to_string()),
        ..ActorAddress::default()
    };

    match response.write_to_bytes() {
        Ok(buf) => send_result(msg_id, buf, session_id, &sessions, &log).await,
        Err(_) => {
            error!(&log, "failed to handle message, todo: send err");
        }
    }
}

async fn session_create_actor(
    msg: CreateActor,
    session_id: Uuid,
    ctx: RemoteActorSystem,
    sessions: LocalActorRef<RemoteSessionStore>,
    log: Logger,
) {
    let msg_id = msg.message_id.clone();

    trace!(
        &log,
        "node_tag={}, node_id={}, message_id={}, received request to create actor",
        ctx.node_tag(),
        ctx.node_id(),
        &msg.message_id
    );
    match ctx.handle_create_actor(msg).await {
        Ok(buf) => {
            send_result(
                msg_id.parse().unwrap(),
                buf.to_vec(),
                session_id,
                &sessions,
                &log,
            )
            .await
        }
        Err(_) => {
            error!(&log, "failed to handle message, todo: send err");
        }
    }
}

async fn session_stream_publish(msg: StreamPublish, sys: RemoteActorSystem, log: Logger) {
    info!(&log, "stream publish");

    // TODO: node should acknowledge the message
    if let Some(mediator) = sys.stream_mediator() {
        mediator.notify::<PublishRaw>(msg.into()).unwrap()
    }
}

async fn send_result(
    msg_id: Uuid,
    res: Vec<u8>,
    session_id: Uuid,
    sessions: &LocalActorRef<RemoteSessionStore>,
    log: &Logger,
) {
    trace!(&log, "sending result");

    let event = ClientEvent::Result(ClientResult {
        message_id: msg_id.to_string(),
        result: res,
        ..ClientResult::default()
    });

    if sessions.send(SessionWrite(session_id, event)).await.is_ok() {
        trace!(&log, "sent result successfully");
    } else {
        error!(&log, "failed to send result");
    }
}
