use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message};
use crate::actor::Actor;
use crate::remote::codec::MessageCodec;
use crate::remote::net::codec::NetworkCodec;
use crate::remote::net::message::ClientEvent;
use crate::remote::net::StreamMessage;
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
    sessions: HashMap<Uuid, RemoteSession>,
}

impl Actor for RemoteSessionStore {}

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
    async fn handle(&mut self, message: NewSession, _ctx: &mut ActorContext) {
        trace!(target: "SessionStore", "new session {}", &message.0.id);
        self.sessions.insert(message.0.id, message.0);
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
        match self.sessions.get_mut(&message.0) {
            Some(session) => {
                trace!(target: "RemoteSession", "writing to session {}", &message.0);

                match message.1.write_to_bytes() {
                    Some(msg) => {
                        trace!(target: "RemoteSession", "message encoded");
                        if session.write.send(msg).await.is_ok() {
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
            None => {
                warn!(target: "RemoteSession", "attempted to write to session that couldn't be found");
            }
        }
    }
}
