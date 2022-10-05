use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message};
use crate::actor::{Actor, IntoActorId, LocalActorRef};
use crate::remote::net::codec::NetworkCodec;
use crate::remote::net::message::{datetime_to_timestamp, ClientEvent};
use crate::remote::net::proto::network::{
    NodeIdentity, RemoteNode as RemoteNodeProto, SystemCapabilities,
};
use crate::remote::net::server::{RemoteServerConfigRef, SessionMessageReceiver};
use crate::remote::net::{receive_loop, StreamData};
use crate::CARGO_PKG_VERSION;
use futures::SinkExt;
use std::collections::HashMap;
use std::net::SocketAddr;
use tokio::io::{ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio_util::codec::{FramedRead, FramedWrite};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub struct RemoteSession {
    id: Uuid,
    addr: SocketAddr,
    write: FramedWrite<WriteHalf<TcpStream>, NetworkCodec>,
    read: Option<FramedRead<ReadHalf<TcpStream>, NetworkCodec>>,
    read_cancellation_token: Option<CancellationToken>,
    remote_server_config: RemoteServerConfigRef,
}

impl RemoteSession {
    pub fn new(
        id: Uuid,
        addr: SocketAddr,
        stream: TcpStream,
        remote_server_config: RemoteServerConfigRef,
    ) -> RemoteSession {
        let (read, write) = tokio::io::split(stream);
        let read = Some(FramedRead::new(read, NetworkCodec));
        let write = FramedWrite::new(write, NetworkCodec);
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

pub struct RemoteSessionStore {
    sessions: HashMap<Uuid, LocalActorRef<RemoteSession>>,
}

impl Actor for RemoteSessionStore {}

#[async_trait]
impl Actor for RemoteSession {
    async fn started(&mut self, ctx: &mut ActorContext) {
        let system = ctx.system().remote();

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
            ..Default::default()
        }))
        .await;

        let _session = tokio::spawn(receive_loop(
            self.addr.to_string(),
            system.clone(),
            self.read.take().unwrap(),
            SessionMessageReceiver::new(self.id, self.actor_ref(ctx)),
        ));
    }

    async fn stopped(&mut self, ctx: &mut ActorContext) {
        info!("session closed (session_id={})", &self.id);

        if let Some(session_store) = ctx.parent::<RemoteSessionStore>() {
            let _ = session_store.send(SessionClosed(self.id)).await;
        }
    }
}

impl RemoteSessionStore {
    pub fn new() -> RemoteSessionStore {
        RemoteSessionStore {
            sessions: HashMap::new(),
        }
    }
}

pub struct NewSession(pub RemoteSession);

pub struct SessionWrite(pub Uuid, pub ClientEvent);

pub struct SessionClosed(pub Uuid);

impl Message for NewSession {
    type Result = LocalActorRef<RemoteSession>;
}

impl Message for SessionClosed {
    type Result = ();
}

impl Message for SessionWrite {
    type Result = ();
}

#[async_trait]
impl Handler<NewSession> for RemoteSessionStore {
    async fn handle(
        &mut self,
        message: NewSession,
        ctx: &mut ActorContext,
    ) -> LocalActorRef<RemoteSession> {
        let session_id = message.0.id;
        let session = message.0;

        let session_actor = ctx
            .spawn(
                format!("RemoteSession-{}", session_id.to_string()).into_actor_id(),
                session,
            )
            .await
            .expect("unable to create session actor");

        let node_id = ctx.system().remote().node_id();
        debug!(target: "SessionStore", node_id = node_id, "new session {}", &session_id);
        self.sessions.insert(session_id, session_actor.clone());
        session_actor
    }
}

#[async_trait]
impl Handler<SessionClosed> for RemoteSessionStore {
    async fn handle(&mut self, message: SessionClosed, ctx: &mut ActorContext) {
        self.sessions.remove(&message.0);
        let node_id = ctx.system().remote().node_id();
        debug!(target: "SessionStore", node_id = node_id,  "disposed session {}", &message.0);
    }
}

#[async_trait]
impl Handler<SessionWrite> for RemoteSessionStore {
    async fn handle(&mut self, message: SessionWrite, _ctx: &mut ActorContext) {
        match self.sessions.get(&message.0) {
            Some(session) => {
                trace!(target: "RemoteSessionStore", "writing to session {}", &message.0);
                if let Err(e) = session.notify(message) {
                    error!(target: "RemoteSessionStore", "error while notifying session of write operation: {}", e);
                }
            }
            None => {
                warn!(target: "RemoteSessionStore", "attempted to write to session that couldn't be found");
            }
        }
    }
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
                trace!(target: "RemoteSession", "message encoded");
                if self.write.send(&msg).await.is_ok() {
                    trace!(target: "RemoteSession", "message sent");
                } else {
                    error!(target: "RemoteSession", "failed to send message");
                }
            }
            None => {
                warn!(target: "RemoteSession", "failed to encode message");
            }
        }
    }
}
