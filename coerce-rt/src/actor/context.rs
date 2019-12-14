use crate::actor::scheduler::ActorScheduler;
use crate::actor::{Actor, ActorId, ActorRef, ActorRefError};
use std::sync::{Arc, Mutex};
use crate::actor::scheduler::actor::RegisterActor;

pub struct ActorContext {
    scheduler: ActorRef<ActorScheduler>,
}

impl ActorContext {
    pub fn new() -> ActorContext {
        ActorContext {
            scheduler: ActorScheduler::new(),
        }
    }

    pub async fn new_actor<A: Actor>(&mut self, actor: A) -> Result<ActorRef<A>, ActorRefError>
    where
        A: 'static + Sync + Send,
    {
        self.scheduler.send(RegisterActor(actor)).await
    }

    pub fn get_actor<A: Actor>(&mut self, id: ActorId) -> Option<ActorRef<A>>
    where
        A: 'static + Sync + Send,
    {
//        self.scheduler.get_actor(&id)
        None
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
