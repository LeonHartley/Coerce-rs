use crate::codec::MessageCodec;
use crate::context::RemoteActorContext;
use crate::net::client::{RemoteClient, RemoteClientErr};
use crate::net::codec::NetworkCodec;
use crate::net::message::{
    ClientError, ClientEvent, ClientHandshake, MessageRequest, SessionEvent,
};
use crate::net::server::session::{
    NewSession, RemoteSession, RemoteSessionStore, SessionClosed, SessionWrite,
};
use crate::net::{receive_loop, StreamReceiver};
use coerce_rt::actor::ActorRef;
use futures::SinkExt;
use serde::Serialize;
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
    sessions: ActorRef<RemoteSessionStore<C>>,
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
        sessions: ActorRef<RemoteSessionStore<C>>,
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
        mut context: RemoteActorContext,
    ) -> Result<(), tokio::io::Error> {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        let (stop_tx, _stop_rx) = tokio::sync::oneshot::channel();

        let session_store = context
            .inner()
            .new_anon_actor(RemoteSessionStore::new(self.codec.clone()))
            .await
            .unwrap();

        tokio::spawn(server_loop(
            listener,
            context,
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
    mut listener: tokio::net::TcpListener,
    context: RemoteActorContext,
    mut session_store: ActorRef<RemoteSessionStore<C>>,
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
                    .await;

                let session = tokio::spawn(receive_loop(
                    context.clone(),
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
    async fn on_recv(&mut self, msg: SessionEvent, ctx: &mut RemoteActorContext) {
        match msg {
            SessionEvent::Handshake(msg) => {
                trace!("server recv {}, {:?}", &msg.node_id, &msg.nodes);

                ctx.register_nodes(msg.nodes).await;
                self.sessions
                    .send(SessionWrite(
                        self.session_id,
                        ClientEvent::Handshake(ClientHandshake {
                            node_id: ctx.node_id(),
                            nodes: ctx.get_nodes().await,
                        }),
                    ))
                    .await;
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
                    .await;
            }
            SessionEvent::Pong(_id) => {}
        }
    }

    async fn on_close(&mut self, ctx: &mut RemoteActorContext) {
        self.sessions.send(SessionClosed(self.session_id)).await;
    }
}

async fn session_handle_message<C: MessageCodec>(
    msg: MessageRequest,
    session_id: Uuid,
    mut ctx: RemoteActorContext,
    mut sessions: ActorRef<RemoteSessionStore<C>>,
) where
    C: 'static + Sync + Send,
{
    match ctx
        .handle(msg.handler_type, msg.actor, msg.message.as_bytes())
        .await
    {
        Ok(buf) => {
            let event = ClientEvent::Result(msg.id, String::from_utf8(buf).expect("string"));
            if sessions.send(SessionWrite(session_id, event)).await.is_ok() {
                trace!(target: "RemoteSession", "sent result successfully");
            } else {
                error!(target: "RemoteSession", "failed to send result");
            }
        }
        Err(_) => {
            error!(target: "RemoteSession", "failed to handle message, todo: send err");
        }
    }
}
