use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message};
use crate::actor::{Actor, LocalActorRef};

use crate::remote::net::codec::NetworkCodec;
use crate::remote::net::message::ClientEvent;
use crate::remote::net::StreamData;
use futures::SinkExt;
use std::collections::HashMap;
use tokio_util::codec::FramedWrite;
use uuid::Uuid;

pub struct RemoteSession {
    id: Uuid,
    write: FramedWrite<tokio::io::WriteHalf<tokio::net::TcpStream>, NetworkCodec>,
}

impl RemoteSession {
    pub fn new(
        id: Uuid,
        write: FramedWrite<tokio::io::WriteHalf<tokio::net::TcpStream>, NetworkCodec>,
    ) -> RemoteSession {
        RemoteSession { id, write }
    }
}

pub struct RemoteSessionStore {
    sessions: HashMap<Uuid, LocalActorRef<RemoteSession>>,
}

impl Actor for RemoteSessionStore {}

impl Actor for RemoteSession {}

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
    type Result = ();
}

impl Message for SessionClosed {
    type Result = ();
}

impl Message for SessionWrite {
    type Result = ();
}

#[async_trait]
impl Handler<NewSession> for RemoteSessionStore {
    async fn handle(&mut self, message: NewSession, ctx: &mut ActorContext) {
        let session_id = message.0.id;
        let session = message.0;

        let session_actor = ctx
            .spawn(session_id.to_string(), session)
            .await
            .expect("unable to create session actor");

        trace!(target: "SessionStore", "new session {}", &session_id);
        self.sessions.insert(session_id, session_actor);
    }
}

#[async_trait]
impl Handler<SessionClosed> for RemoteSessionStore {
    async fn handle(&mut self, message: SessionClosed, _ctx: &mut ActorContext) {
        self.sessions.remove(&message.0);
        trace!(target: "SessionStore", "disposed session {}", &message.0);
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
        match message.1.write_to_bytes() {
            Some(msg) => {
                trace!(target: "RemoteSession", "message encoded");
                if self.write.send(msg).await.is_ok() {
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
