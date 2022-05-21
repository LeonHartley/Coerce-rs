use crate::remote::system::RemoteActorSystem;

use crate::actor::{ActorId, LocalActorRef};
use crate::remote::net::codec::NetworkCodec;
use crate::remote::net::message::{
    datetime_to_timestamp, timestamp_to_datetime, ClientEvent, SessionEvent,
};
use crate::remote::net::server::session::{
    NewSession, RemoteSession, RemoteSessionStore, SessionClosed, SessionWrite,
};
use crate::remote::net::{receive_loop, StreamReceiver};

use crate::actor::scheduler::ActorType::Anonymous;
use crate::remote::actor::RemoteResponse;
use crate::remote::cluster::discovery::{Discover, Seed};
use crate::remote::cluster::node::RemoteNode;
use crate::remote::net::proto::network::{
    ActorAddress, ClientHandshake, ClientResult, CreateActor, MessageRequest, NodeIdentity, Pong,
    RemoteNode as RemoteNodeProto, SessionHandshake, StreamPublish,
};
use crate::remote::stream::mediator::PublishRaw;
use crate::CARGO_PKG_VERSION;

use futures::future::FutureExt;
use opentelemetry::global;
use protobuf::Message;
use std::collections::HashMap;
use std::net::SocketAddr;

use std::str::FromStr;
use std::sync::Arc;

use tokio::io;

use tokio::sync::oneshot;

use tokio_util::codec::{FramedRead, FramedWrite};
use tokio_util::sync::CancellationToken;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;

pub mod session;

pub struct RemoteServer {
    cancellation_token: Option<CancellationToken>,
}

pub struct SessionMessageReceiver {
    session_id: Uuid,
    sessions: LocalActorRef<RemoteSessionStore>,
    session: LocalActorRef<RemoteSession>,
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
        session: LocalActorRef<RemoteSession>,
    ) -> SessionMessageReceiver {
        SessionMessageReceiver {
            session_id,
            sessions,
            session,
        }
    }
}

impl RemoteServer {
    pub fn new() -> Self {
        RemoteServer {
            cancellation_token: None,
        }
    }

    pub async fn start(
        &mut self,
        addr: String,
        system: RemoteActorSystem,
    ) -> Result<(), tokio::io::Error> {
        let listener = tokio::net::TcpListener::bind(&addr).await?;

        let session_store = system
            .actor_system()
            .new_actor(
                format!("RemoteSessionStore-{}", system.node_tag()),
                RemoteSessionStore::new(),
                Anonymous,
            )
            .await
            .unwrap();

        let token = CancellationToken::new();
        tokio::spawn(server_loop(
            listener,
            system,
            session_store,
            token.clone(),
            addr,
        ));
        self.cancellation_token = Some(token);
        Ok(())
    }

    pub fn stop(&mut self) {
        if let Some(cancellation_token) = self.cancellation_token.take() {
            cancellation_token.cancel();
        }
    }
}

pub async fn cancellation(cancellation_token: CancellationToken) {
    cancellation_token.cancelled().await
}

pub async fn accept(
    listener: &tokio::net::TcpListener,
    cancellation_token: CancellationToken,
) -> Option<io::Result<(tokio::net::TcpStream, SocketAddr)>> {
    tokio::select! {
        _ = cancellation(cancellation_token) => {
            None
        }

        res = listener.accept() => {
            Some(res)
        }
    }
}

pub async fn server_loop(
    listener: tokio::net::TcpListener,
    system: RemoteActorSystem,
    session_store: LocalActorRef<RemoteSessionStore>,
    cancellation_token: CancellationToken,
    node_addr: String,
) {
    loop {
        match accept(&listener, cancellation_token.clone()).await {
            Some(Ok((socket, addr))) => {
                trace!(target: "RemoteServer", "client accepted {}", addr);
                let (read, write) = tokio::io::split(socket);

                let read = FramedRead::new(read, NetworkCodec);
                let write = FramedWrite::new(write, NetworkCodec);

                let session_id = Uuid::new_v4();

                let session = session_store
                    .send(NewSession(RemoteSession::new(session_id, write)))
                    .await
                    .expect("new session registered");

                let peers = system
                    .get_nodes()
                    .await
                    .into_iter()
                    .map(|node| RemoteNodeProto {
                        node_id: node.id,
                        addr: node.addr,
                        tag: node.tag,
                        node_started_at: node
                            .node_started_at
                            .as_ref()
                            .map(datetime_to_timestamp)
                            .into(),
                        ..RemoteNodeProto::default()
                    })
                    .collect::<Vec<RemoteNodeProto>>();

                session
                    .send(SessionWrite(
                        session_id,
                        ClientEvent::Identity(NodeIdentity {
                            node_id: system.node_id(),
                            node_tag: system.node_tag().to_string(),
                            application_version: format!(
                                "pkg_version={},protocol_version=1",
                                &CARGO_PKG_VERSION
                            ),
                            addr: node_addr.clone(),
                            node_started_at: Some(datetime_to_timestamp(system.started_at()))
                                .into(),
                            peers: peers.into(),
                            ..Default::default()
                        }),
                    ))
                    .await
                    .expect("send session write");

                let _session = tokio::spawn(receive_loop(
                    system.clone(),
                    read,
                    SessionMessageReceiver::new(session_id, session_store.clone(), session),
                ));
            }
            Some(Err(e)) => error!(target: "RemoteServer", "error accepting client: {:?}", e),
            None => break,
        }
    }

    info!("tcp listener {:?} stopped", &listener)
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

                trace!(target: "RemoteServer", "received identify from node (id={}, tag={}), session_id={}", &identify.source_node_id, &identify.source_node_tag, &self.session_id);

                tokio::spawn(async move {});
            }
            SessionEvent::Handshake(msg) => {
                trace!(target: "RemoteServer", "handshake {}, {:?}, type: {:?}", &msg.node_id, &msg.nodes, &msg.client_type);
                tokio::spawn(session_handshake(
                    sys.clone(),
                    msg,
                    self.session_id,
                    self.session.clone(),
                ));
            }

            SessionEvent::FindActor(find_actor) => {
                trace!(target: "RemoteServer", "actor lookup {}, {}", &self.session_id, &find_actor.actor_id);
                tokio::spawn(session_handle_lookup(
                    Uuid::from_str(&find_actor.message_id).unwrap(),
                    find_actor.actor_id,
                    self.session_id,
                    sys.clone(),
                    self.session.clone(),
                ));
            }

            SessionEvent::RegisterActor(actor) => {
                trace!(target: "RemoteServer", "register actor {}, {}", &actor.actor_id, &actor.node_id);
                let node_id = actor.get_node_id();
                sys.register_actor(actor.actor_id, Some(node_id));
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
                trace!(target: "RemoteServer", "ping received, sending pong");
                self.session
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
                trace!(target: "RemoteServer", "create actor {}, {:?}", self.session_id, &msg.actor_id);
                tokio::spawn(session_create_actor(
                    msg,
                    self.session_id,
                    sys.clone(),
                    self.session.clone(),
                ));
            }

            SessionEvent::StreamPublish(msg) => {
                trace!(target: "RemoteServer", "stream publish {}, {:?}", self.session_id, &msg);
                tokio::spawn(session_stream_publish(msg, sys.clone()));
            }

            SessionEvent::Raft(req) => {
                if let Some(raft) = sys.raft() {
                    raft.inbound_request(req, self.session_id, &self.sessions)
                        .await
                }
            }
            SessionEvent::Result(res) => {
                match sys.pop_request(Uuid::from_str(&res.message_id).unwrap()) {
                    Some(res_tx) => {
                        let _ = res_tx.send(RemoteResponse::Ok(res.result));
                    }
                    None => {
                        warn!(target: "RemoteServer", "node_tag={}, node_id={}, received unknown request result (id={})", sys.node_tag(), sys.node_id(), res.message_id);
                    }
                }
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
    session: LocalActorRef<RemoteSession>,
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
    let mut response = ClientHandshake {
        node_id: ctx.node_id(),
        node_tag: ctx.node_tag().to_string(),
        node_started_at: Some(datetime_to_timestamp(ctx.started_at())).into(),
        ..ClientHandshake::default()
    };

    for node in nodes {
        response.nodes.push(RemoteNodeProto {
            node_id: node.id,
            addr: node.addr,
            tag: node.tag,
            node_started_at: node
                .node_started_at
                .as_ref()
                .map(datetime_to_timestamp)
                .into(),
            ..RemoteNodeProto::default()
        });
    }

    let nodes = handshake
        .nodes
        .into_iter()
        .map(|n| {
            let started_at = n.node_started_at.into_option().map(timestamp_to_datetime);
            RemoteNode::new(n.node_id, n.addr, n.tag, started_at)
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
    session_id: Uuid,
    ctx: RemoteActorSystem,
    session: LocalActorRef<RemoteSession>,
) {
    let mut headers = HashMap::<String, String>::new();
    headers.insert("traceparent".to_owned(), msg.trace_id);
    let span = tracing::trace_span!("RemoteServer::MessageRequest");
    span.set_parent(global::get_text_map_propagator(|propagator| {
        propagator.extract(&mut headers)
    }));

    let _enter = span.enter();

    match ctx
        .handle_message(
            msg.handler_type.as_str(),
            msg.actor_id.clone(),
            msg.message.as_slice(),
        )
        .await
    {
        Ok(buf) => send_result(msg.message_id.parse().unwrap(), buf, session_id, session).await,
        Err(e) => {
            error!(target: "RemoteSession", "[node={}] failed to handle message (handler_type={}, target_actor_id={}), error={:?}", ctx.node_id(), &msg.handler_type, &msg.actor_id, e);
            // TODO: Send error
        }
    }
}

async fn session_handle_lookup(
    msg_id: Uuid,
    id: ActorId,
    session_id: Uuid,
    ctx: RemoteActorSystem,
    session: LocalActorRef<RemoteSession>,
) {
    let node_id = ctx.locate_actor_node(id.clone()).await;
    trace!(target: "RemoteSession", "sending actor lookup result: {:?}", node_id);

    let response = ActorAddress {
        actor_id: id,
        node_id: node_id.unwrap_or(0),
        ..ActorAddress::default()
    };

    match response.write_to_bytes() {
        Ok(buf) => send_result(msg_id, buf, session_id, session).await,
        Err(_) => {
            error!(target: "RemoteSession", "failed to handle message, todo: send err");
        }
    }
}

async fn session_create_actor(
    msg: CreateActor,
    session_id: Uuid,
    ctx: RemoteActorSystem,
    session: LocalActorRef<RemoteSession>,
) {
    let msg_id = msg.message_id.clone();
    let actor_id = if msg.actor_id.is_empty() {
        None
    } else {
        Some(msg.actor_id)
    };

    trace!(target: "RemoteSession", "node_tag={}, node_id={}, message_id={}, received request to create actor (actor_id={})", ctx.node_tag(), ctx.node_id(), &msg.message_id, actor_id.as_ref().map_or_else(|| "N/A", |s| s));
    match ctx
        .handle_create_actor(actor_id, msg.actor_type, msg.recipe, None)
        .await
    {
        Ok(buf) => send_result(msg_id.parse().unwrap(), buf.to_vec(), session_id, session).await,
        Err(_) => {
            error!(target: "RemoteSession", "failed to handle message, todo: send err");
        }
    }
}

async fn session_stream_publish(msg: Arc<StreamPublish>, sys: RemoteActorSystem) {
    // TODO: node should acknowledge the message
    if let Some(mediator) = sys.stream_mediator() {
        mediator.notify::<PublishRaw>(msg.into()).unwrap()
    }
}

async fn send_result(
    msg_id: Uuid,
    res: Vec<u8>,
    session_id: Uuid,
    session: LocalActorRef<RemoteSession>,
) {
    trace!(target: "RemoteSession", "sending result");

    let event = ClientEvent::Result(ClientResult {
        message_id: msg_id.to_string(),
        result: res,
        ..ClientResult::default()
    });

    if session.send(SessionWrite(session_id, event)).await.is_ok() {
        trace!(target: "RemoteSession", "sent result successfully");
    } else {
        error!(target: "RemoteSession", "failed to send result");
    }
}
