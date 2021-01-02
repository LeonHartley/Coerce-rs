use crate::remote::system::RemoteActorSystem;

use crate::actor::{ActorId, LocalActorRef};
use crate::remote::net::codec::NetworkCodec;
use crate::remote::net::message::{ClientEvent, SessionEvent};
use crate::remote::net::server::session::{
    NewSession, RemoteSession, RemoteSessionStore, SessionClosed, SessionWrite,
};
use crate::remote::net::{receive_loop, StreamReceiver};

use crate::remote::cluster::node::RemoteNode;
use crate::remote::net::proto::protocol::{
    ActorAddress, ClientHandshake, ClientResult, CreateActor, MessageRequest, Pong,
    RemoteNode as RemoteNodeProto, SessionHandshake, StreamPublish,
};
use protobuf::Message;
use std::str::FromStr;
use tokio_util::codec::{FramedRead, FramedWrite};
use uuid::Uuid;
use crate::remote::stream::mediator::PublishRaw;

pub mod session;

pub struct RemoteServer {
    stop: Option<tokio::sync::oneshot::Sender<bool>>,
}

pub struct SessionMessageReceiver {
    session_id: Uuid,
    sessions: LocalActorRef<RemoteSessionStore>,
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
    ) -> SessionMessageReceiver {
        SessionMessageReceiver {
            session_id,
            sessions,
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
        mut system: RemoteActorSystem,
    ) -> Result<(), tokio::io::Error> {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        let (stop_tx, _stop_rx) = tokio::sync::oneshot::channel();

        let session_store = system
            .inner()
            .new_anon_actor(RemoteSessionStore::new())
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
    mut session_store: LocalActorRef<RemoteSessionStore>,
) {
    loop {
        match listener.accept().await {
            Ok((socket, addr)) => {
                trace!(target: "RemoteServer", "client accepted {}", addr);
                let (read, write) = tokio::io::split(socket);

                let read = FramedRead::new(read, NetworkCodec);
                let write = FramedWrite::new(write, NetworkCodec);
                let (_stop_tx, stop_rx) = tokio::sync::oneshot::channel();

                let session_id = Uuid::new_v4();

                session_store
                    .send(NewSession(RemoteSession::new(session_id, write)))
                    .await
                    .expect("new session registered");

                let _session = tokio::spawn(receive_loop(
                    system.clone(),
                    read,
                    stop_rx,
                    SessionMessageReceiver::new(session_id, session_store.clone()),
                ));
            }
            Err(e) => error!(target: "RemoteServer", "error accepting client: {:?}", e),
        }
    }
}

#[async_trait]
impl StreamReceiver for SessionMessageReceiver {
    type Message = SessionEvent;

    async fn on_recv(&mut self, msg: SessionEvent, ctx: &mut RemoteActorSystem) {
        match msg {
            SessionEvent::Handshake(msg) => {
                trace!(target: "RemoteServer", "handshake {}, {:?}", &msg.node_id, &msg.nodes);
                tokio::spawn(session_handshake(
                    ctx.clone(),
                    msg,
                    self.session_id,
                    self.sessions.clone(),
                ));
            }

            SessionEvent::FindActor(find_actor) => {
                trace!(target: "RemoteServer", "actor lookup {}, {}", &self.session_id, &find_actor.actor_id);
                tokio::spawn(session_handle_lookup(
                    Uuid::from_str(&find_actor.message_id).unwrap(),
                    find_actor.actor_id,
                    self.session_id,
                    ctx.clone(),
                    self.sessions.clone(),
                ));
            }

            SessionEvent::RegisterActor(actor) => {
                let node_id = Uuid::from_str(actor.get_node_id()).unwrap();

                trace!(target: "RemoteServer", "register actor {}, {}", &actor.actor_id, &actor.node_id);
                ctx.register_actor(actor.actor_id, Some(node_id));
            }

            SessionEvent::NotifyActor(msg) => {
                tokio::spawn(session_handle_message(
                    msg,
                    self.session_id,
                    ctx.clone(),
                    self.sessions.clone(),
                ));
            }

            SessionEvent::Ping(ping) => {
                trace!(target: "RemoteServer", "ping received, sending pong");
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
                trace!(target: "RemoteServer", "create actor {}, {:?}", self.session_id, &msg.actor_id);
                tokio::spawn(session_create_actor(
                    msg,
                    self.session_id,
                    ctx.clone(),
                    self.sessions.clone(),
                ));
            }

            SessionEvent::StreamPublish(msg) => {
                trace!(target: "RemoteServer", "stream publish {}, {:?}", self.session_id, &msg);
                tokio::spawn(session_stream_publish(msg, ctx.clone()));
            }
        }
    }

    async fn on_close(&mut self, _ctx: &mut RemoteActorSystem) {
        self.sessions
            .send(SessionClosed(self.session_id))
            .await
            .expect("send session closed")
    }
}

async fn session_handshake(
    mut ctx: RemoteActorSystem,
    handshake: SessionHandshake,
    session_id: Uuid,
    mut sessions: LocalActorRef<RemoteSessionStore>,
) {
    let nodes = ctx.get_nodes().await;
    let mut response = ClientHandshake {
        node_id: ctx.node_id().to_string(),
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
    mut ctx: RemoteActorSystem,
    mut sessions: LocalActorRef<RemoteSessionStore>,
) {
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
            )
            .await
        }
        Err(_) => {
            error!(target: "RemoteSession", "failed to handle message, todo: send err");
        }
    }
}

async fn session_handle_lookup(
    msg_id: Uuid,
    id: ActorId,
    session_id: Uuid,
    mut ctx: RemoteActorSystem,
    mut sessions: LocalActorRef<RemoteSessionStore>,
) {
    let node_id = ctx.locate_actor_node(id.clone()).await;
    trace!(target: "RemoteSession", "sending actor lookup result: {:?}", node_id);

    let response = ActorAddress {
        actor_id: id,
        node_id: node_id.map_or_else(|| String::default(), |n| n.to_string()),
        ..ActorAddress::default()
    };

    match response.write_to_bytes() {
        Ok(buf) => send_result(msg_id, buf, session_id, &mut sessions).await,
        Err(_) => {
            error!(target: "RemoteSession", "failed to handle message, todo: send err");
        }
    }
}

async fn session_create_actor(
    msg: CreateActor,
    session_id: Uuid,
    mut ctx: RemoteActorSystem,
    mut sessions: LocalActorRef<RemoteSessionStore>,
) {
    let msg_id = msg.message_id.clone();
    match ctx.handle_create_actor(msg).await {
        Ok(buf) => send_result(msg_id.parse().unwrap(), buf, session_id, &mut sessions).await,
        Err(_) => {
            error!(target: "RemoteSession", "failed to handle message, todo: send err");
        }
    }
}

async fn session_stream_publish(msg: StreamPublish, mut ctx: RemoteActorSystem) {
    info!("stream publish");

    // TODO: node should acknowledge the message
    if let Some(mediator) = ctx.mediator_ref.as_mut() {
        mediator.notify::<PublishRaw>(msg.into()).unwrap()
    }
}

async fn send_result(
    msg_id: Uuid,
    res: Vec<u8>,
    session_id: Uuid,
    sessions: &mut LocalActorRef<RemoteSessionStore>,
) {
    trace!(target: "RemoteSession", "sending result");
    let event = ClientEvent::Result(ClientResult {
        message_id: msg_id.to_string(),
        result: res,
        ..ClientResult::default()
    });

    if sessions.send(SessionWrite(session_id, event)).await.is_ok() {
        trace!(target: "RemoteSession", "sent result successfully");
    } else {
        error!(target: "RemoteSession", "failed to send result");
    }
}
