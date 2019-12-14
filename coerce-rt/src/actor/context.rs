use crate::actor::scheduler::{ActorScheduler, GetActor, RegisterActor};
use crate::actor::{Actor, ActorId, ActorRef, ActorRefError};
use std::sync::{Arc, Mutex};

lazy_static! {
    static ref CURRENT_CONTEXT: ActorContext = { ActorContext::new() };
}

#[derive(Clone)]
pub struct ActorContext {
    scheduler: ActorRef<ActorScheduler>,
}

impl ActorContext {
    pub fn new() -> ActorContext {
        ActorContext {
            scheduler: ActorScheduler::new(),
        }
    }

    pub fn current_context() -> ActorContext {
        CURRENT_CONTEXT.clone()
    }

    pub async fn new_actor<A: Actor>(&mut self, actor: A) -> Result<ActorRef<A>, ActorRefError>
    where
        A: 'static + Sync + Send,
    {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let actor_ref = self.scheduler.send(RegisterActor(actor, tx)).await;

        match rx.await {
            Ok(true) => actor_ref,
            _ => Err(ActorRefError::ActorUnavailable),
        }
    }

    pub async fn get_actor<A: Actor>(&mut self, id: ActorId) -> Option<ActorRef<A>>
    where
        A: 'static + Sync + Send,
    {
        match self.scheduler.send(GetActor::new(id)).await {
            Ok(a) => a,
            Err(_) => None,
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum ActorStatus {
    Starting,
    Started,
    Stopping,
    Stopped,
}

pub struct ActorHandlerContext {
    status: ActorStatus,
    timers: Vec<tokio::time::Interval>,
}

impl ActorHandlerContext {
    pub fn new(status: ActorStatus) -> ActorHandlerContext {
        ActorHandlerContext {
            status,
            timers: vec![],
        }
    }

    pub fn set_status(&mut self, state: ActorStatus) {
        self.status = state
    }

    pub fn get_status(&self) -> &ActorStatus {
        &self.status
    }
}
