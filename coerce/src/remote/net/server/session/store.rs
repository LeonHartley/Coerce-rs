use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message};
use crate::actor::{Actor, IntoActorId, LocalActorRef};
use crate::remote::net::message::ClientEvent;
use crate::remote::net::server::session::RemoteSession;
use std::collections::HashMap;
use uuid::Uuid;

pub struct RemoteSessionStore {
    sessions: HashMap<Uuid, LocalActorRef<RemoteSession>>,
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
        debug!(node_id = node_id, "new session {}", &session_id);
        self.sessions.insert(session_id, session_actor.clone());
        session_actor
    }
}

#[async_trait]
impl Handler<SessionClosed> for RemoteSessionStore {
    async fn handle(&mut self, message: SessionClosed, ctx: &mut ActorContext) {
        self.sessions.remove(&message.0);
        let node_id = ctx.system().remote().node_id();
        debug!(node_id = node_id, "disposed session {}", &message.0);
    }
}

#[async_trait]
impl Handler<SessionWrite> for RemoteSessionStore {
    async fn handle(&mut self, message: SessionWrite, _ctx: &mut ActorContext) {
        match self.sessions.get(&message.0) {
            Some(session) => {
                trace!("writing to session {}", &message.0);
                if let Err(e) = session.notify(message) {
                    error!("error while notifying session of write operation: {}", e);
                }
            }
            None => {
                warn!("attempted to write to session that couldn't be found");
            }
        }
    }
}
