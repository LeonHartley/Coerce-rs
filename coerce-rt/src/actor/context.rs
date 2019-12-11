use crate::actor::scheduler::{ActorRef, ActorScheduler};
use crate::actor::{Actor, ActorId};
use std::sync::{Arc, Mutex};

pub struct ActorContext {
    ctx: Option<Arc<Mutex<ActorContext>>>,
    scheduler: Arc<ActorScheduler>,
}

impl ActorContext {
    pub fn new() -> Arc<Mutex<ActorContext>> {
        let base = ActorContext {
            ctx: None,
            scheduler: Arc::new(ActorScheduler::new()),
        };
        let mut ctx = Arc::new(Mutex::new(base));

        ctx.lock().unwrap().ctx = Some(ctx.clone());

        ctx
    }

    pub fn new_actor<A: Actor + Sync + Send + 'static>(&self, actor: A) -> ActorRef<A> {
        self.scheduler
            .register(actor, self.ctx.as_ref().unwrap().clone())
    }
}
