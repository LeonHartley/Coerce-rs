use crate::remote::codec::MessageCodec;
use crate::remote::system::RemoteActorSystem;

use crate::actor::{ActorId, LocalActorRef};
use crate::remote::net::codec::NetworkCodec;
use crate::remote::net::message::{
    ActorNode, ClientEvent, ClientHandshake, CreateActor, MessageRequest, SessionEvent,
    SessionHandshake,
};
use crate::remote::net::server::session::{
    NewSession, RemoteSession, RemoteSessionStore, SessionClosed, SessionWrite,
};
use crate::remote::net::{receive_loop, StreamReceiver};

use tokio_util::codec::{FramedRead, FramedWrite};
use uuid::Uuid;

pub mod session;

pub struct RemoteServer<C: MessageCodec> {
    codec: C,
    stop: Option<tokio::sync::oneshot::Sender<bool>>,
}

pub struct SessionMessageReceiver<C: MessageCodec>
where
    C: 'static + Sync + Send,
{
    session_id: Uuid,
    sessions: LocalActorRef<RemoteSessionStore<C>>,
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

impl<C: MessageCodec> SessionMessageReceiver<C>
where
    C: Sync + Send,
{
    pub fn new(
        session_id: Uuid,
        sessions: LocalActorRef<RemoteSessionStore<C>>,
    ) -> SessionMessageReceiver<C> {
        SessionMessageReceiver {
            session_id,
            sessions,
        }
    }
}

impl<C: MessageCodec> RemoteServer<C>
where
    C: 'static + Sync + Send,
{
    pub fn new(codec: C) -> Self {
        RemoteServer { codec, stop: None }
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
            .new_anon_actor(RemoteSessionStore::new(self.codec.clone()))
            .await
            .unwrap();

        tokio::spawn(server_loop(
            listener,
            system,
            session_store,
            self.codec.clone(),
        ));
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

pub async fn server_loop<C: MessageCodec>(
    listener: tokio::net::TcpListener,
    system: RemoteActorSystem,
    mut session_store: LocalActorRef<RemoteSessionStore<C>>,
    codec: C,
) where
    C: 'static + Sync + Send,
{
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
                    codec.clone(),
                ));
            }
            Err(e) => error!(target: "RemoteServer", "error accepting client: {:?}", e),
        }
    }
}

#[async_trait]
impl<C: MessageCodec> StreamReceiver<SessionEvent> for SessionMessageReceiver<C>
where
    C: Sync + Send,
{
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

            SessionEvent::ActorLookup(msg_id, actor_id) => {
                trace!(target: "RemoteServer", "actor lookup {}, {}", &self.session_id, actor_id);
                tokio::spawn(session_handle_lookup(
                    msg_id,
                    actor_id,
                    self.session_id,
                    ctx.clone(),
                    self.sessions.clone(),
                ));
            }

            SessionEvent::RegisterActor(actor) => {
                trace!(target: "RemoteServer", "register actor {}, {}", &actor.id, &actor.node_id);
                ctx.register_actor(actor.id, Some(actor.node_id)).await;
            }

            SessionEvent::Message(msg) => {
                tokio::spawn(session_handle_message(
                    msg,
                    self.session_id,
                    ctx.clone(),
                    self.sessions.clone(),
                ));
            }

            SessionEvent::Ping(id) => {
                trace!(target: "RemoteServer", "ping received, sending pong");
                self.sessions
                    .send(SessionWrite(self.session_id, ClientEvent::Pong(id)))
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
            _ => {}
        }
    }

    async fn on_close(&mut self, _ctx: &mut RemoteActorSystem) {
        self.sessions
            .send(SessionClosed(self.session_id))
            .await
            .expect("send session closed")
    }
}

async fn session_handshake<C: MessageCodec>(
    mut ctx: RemoteActorSystem,
    handshake: SessionHandshake,
    session_id: Uuid,
    mut sessions: LocalActorRef<RemoteSessionStore<C>>,
) where
    C: 'static + Sync + Send,
{
    let nodes = ctx.get_nodes().await;
    sessions
        .send(SessionWrite(
            session_id,
            ClientEvent::Handshake(ClientHandshake {
                node_id: ctx.node_id(),
                nodes,
            }),
        ))
        .await
        .expect("send session write (handshake)");

    ctx.register_nodes(handshake.nodes).await;
}

async fn session_handle_message<C: MessageCodec>(
    msg: MessageRequest,
    session_id: Uuid,
    mut ctx: RemoteActorSystem,
    mut sessions: LocalActorRef<RemoteSessionStore<C>>,
) where
    C: 'static + Sync + Send,
{
    match ctx
        .handle_message(msg.handler_type, msg.actor, msg.message.as_slice())
        .await
    {
        Ok(buf) => send_result(msg.id, buf, session_id, &mut sessions).await,
        Err(_) => {
            error!(target: "RemoteSession", "failed to handle message, todo: send err");
        }
    }
}

async fn session_handle_lookup<C: MessageCodec>(
    msg_id: Uuid,
    id: ActorId,
    session_id: Uuid,
    mut ctx: RemoteActorSystem,
    mut sessions: LocalActorRef<RemoteSessionStore<C>>,
) where
    C: 'static + Sync + Send,
{
    let node_id = ctx.locate_actor(id.clone()).await;
    trace!(target: "RemoteSession", "sending actor lookup result");
    match serde_json::to_vec(&ActorNode { node_id, id }) {
        Ok(buf) => send_result(msg_id, buf, session_id, &mut sessions).await,
        Err(_) => {
            error!(target: "RemoteSession", "failed to handle message, todo: send err");
        }
    }
}

async fn session_create_actor<C: MessageCodec>(
    msg: CreateActor,
    session_id: Uuid,
    mut ctx: RemoteActorSystem,
    mut sessions: LocalActorRef<RemoteSessionStore<C>>,
) where
    C: 'static + Sync + Send,
{
    let msg_id = msg.id;
    match ctx.handle_create_actor(msg).await {
        Ok(buf) => send_result(msg_id, buf, session_id, &mut sessions).await,
        Err(_) => {
            error!(target: "RemoteSession", "failed to handle message, todo: send err");
        }
    }
}

async fn send_result<C: MessageCodec>(
    msg_id: Uuid,
    res: Vec<u8>,
    session_id: Uuid,
    sessions: &mut LocalActorRef<RemoteSessionStore<C>>,
) where
    C: 'static + Sync + Send,
{
    trace!(target: "RemoteSession", "sending result");
    let event = ClientEvent::Result(msg_id, res);

    if sessions.send(SessionWrite(session_id, event)).await.is_ok() {
        trace!(target: "RemoteSession", "sent result successfully");
    } else {
        error!(target: "RemoteSession", "failed to send result");
    }
}
