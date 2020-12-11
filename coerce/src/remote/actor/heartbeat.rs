// use chrono::{DateTime, Utc};
// use crate::actor::context::ActorSystem;
// use crate::actor::scheduler::timer::TimerTick;
// use crate::actor::scheduler::ActorType::Tracked;
// use crate::actor::{Actor, ActorId, LocalActorRef};
// use std::collections::{HashMap, HashSet};
//
// pub struct ActorHeartbeat {
//     last_heartbeat: DateTime<Utc>,
// }
//
// pub struct RemoteActorRegistry {
//     actors: HashMap<ActorId, ActorHeartbeat>,
// }
//
// impl Actor for RemoteActorRegistry {}
//
// impl RemoteActorRegistry {
//     pub fn new(mut system: ActorSystem) -> LocalActorRef<RemoteActorRegistry> {
//         let registry = RemoteActorRegistry {
//             actors: HashMap::new(),
//         };
//
//         context
//             .new_actor(format!("RemoteActorRegistry-0"), registry, Tracked)
//             .unwrap()
//     }
// }
//
// pub struct RemoteActorTick;
//
// impl TimerTick for RemoteActorTick {}
