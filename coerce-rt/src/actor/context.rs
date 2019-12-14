use crate::actor::scheduler::{ActorScheduler, GetActor, RegisterActor};
use crate::actor::{Actor, ActorId, ActorRef, ActorRefError};
use std::sync::{Arc, Mutex};

lazy_static! {
    static ref CURRENT_CONTEXT: ActorContext = { println!("test"); ActorContext::new() };
}

pub struct ActorContext {
    scheduler: ActorRef<ActorScheduler>,
}

impl ActorContext {
    pub fn new() -> ActorContext {
        ActorContext {
            scheduler: ActorScheduler::new(),
        }
    }

    pub fn current_scheduler() -> ActorRef<ActorScheduler> {
        CURRENT_CONTEXT.scheduler.clone()
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
