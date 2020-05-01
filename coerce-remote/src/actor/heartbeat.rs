use chrono::{DateTime, Utc};
use coerce_rt::actor::context::ActorContext;
use coerce_rt::actor::scheduler::ActorType::Tracked;
use coerce_rt::actor::{Actor, ActorId, ActorRef};
use std::collections::{HashMap, HashSet};
use coerce_rt::actor::scheduler::timer::TimerTick;

pub struct ActorHeartbeat {
    last_heartbeat: DateTime<Utc>,
}

pub struct RemoteActorRegistry {
    actors: HashMap<ActorId, ActorHeartbeat>,
}

impl Actor for RemoteActorRegistry {}

impl RemoteActorRegistry {
    pub fn new(mut context: ActorContext) -> ActorRef<RemoteActorRegistry> {
        let registry = RemoteActorRegistry {
            actors: HashMap::new(),
        };

        context
            .new_actor(format!("RemoteActorRegistry-0"), registry, Tracked)
            .unwrap()
    }
}

pub struct RemoteActorTick;

impl TimerTick for RemoteActorTick {

}

impl
