use crate::actor::scheduler::{ActorRef, ActorScheduler};
use crate::actor::{Actor, ActorId};
use std::sync::{Arc, Mutex};

pub struct ActorContext {
    ctx: Option<Arc<Mutex<ActorContext>>>,
    scheduler: ActorScheduler,
}

impl ActorContext {
    pub fn new() -> Arc<Mutex<ActorContext>> {
        let base = ActorContext {
            ctx: None,
            scheduler: ActorScheduler::new(),
        };

        let ctx = Arc::new(Mutex::new(base));

        ctx.lock().unwrap().ctx = Some(ctx.clone());

        ctx
    }

    pub fn new_actor<A: Actor + Sync + Send + 'static>(&mut self, actor: A) -> ActorRef<A> {
        self.scheduler
            .register(actor, self.ctx.as_ref().unwrap().clone())
    }

    pub fn get_actor<A: Actor + Sync + Send + 'static>(&mut self, id: ActorId) -> Option<ActorRef<A>> {
        self.scheduler.get_actor(&id)
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
}

impl ActorHandlerContext {
    pub fn new(status: ActorStatus) -> ActorHandlerContext {
        ActorHandlerContext { status }
    }

    pub fn set_status(&mut self, state: ActorStatus) {
        self.status = state
    }

    pub fn get_status(&self) -> &ActorStatus {
        &self.status
    }
}
