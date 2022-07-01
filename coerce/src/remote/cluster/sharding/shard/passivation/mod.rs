use crate::actor::context::{ActorContext, ActorStatus};
use crate::actor::message::{Handler, Message};
use crate::actor::scheduler::timer::{Timer, TimerTick};
use crate::actor::system::ActorSystem;
use crate::actor::{Actor, ActorId, BoxedActorRef, LocalActorRef};
use crate::remote::cluster::sharding::shard::Shard;
use std::collections::HashMap;
use std::time::{Duration, Instant};

pub struct PassivationConfig {
    entity_passivation_tick: Duration,
    entity_passivation_timeout: Duration,
    entity_deletion_timeout: Option<Duration>,
}

pub struct PassivationWorker {
    shard: LocalActorRef<Shard>,
    config: PassivationConfig,
    timer: Option<Timer>,
}

impl PassivationWorker {
    pub fn new(shard: LocalActorRef<Shard>, config: PassivationConfig) -> Self {
        let timer = None;
        PassivationWorker {
            shard,
            config,
            timer,
        }
    }
}

#[derive(Clone)]
struct PassivationTimerTick;

impl Message for PassivationTimerTick {
    type Result = ();
}

impl TimerTick for PassivationTimerTick {}

#[async_trait]
impl Actor for PassivationWorker {
    async fn started(&mut self, ctx: &mut ActorContext) {
        self.timer = Some(Timer::start(
            self.actor_ref(ctx),
            self.config.entity_passivation_tick,
            PassivationTimerTick,
        ));
    }

    async fn stopped(&mut self, _ctx: &mut ActorContext) {
        if let Some(entity_passivation_timer) = self.timer.take() {
            let _ = entity_passivation_timer.stop();
        }
    }
}
#[async_trait]
impl Handler<PassivationTimerTick> for PassivationWorker {
    async fn handle(&mut self, _message: PassivationTimerTick, ctx: &mut ActorContext) {}
}
