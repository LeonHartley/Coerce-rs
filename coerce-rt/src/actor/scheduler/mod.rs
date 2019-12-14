use crate::actor::context::{ActorContext, ActorStatus};
use crate::actor::lifecycle::{actor_loop, Status, Stop};
use crate::actor::message::{
    ActorMessage, ActorMessageHandler, Exec, Handler, Message, MessageHandler, MessageResult,
};
use crate::actor::{Actor, ActorId, ActorRef, BoxedActorRef};
use std::any::Any;

use std::collections::HashMap;

use std::sync::{Arc, Mutex};
use uuid::Uuid;
use crate::actor::scheduler::actor::start_actor;

pub mod actor;
pub mod timer;

pub struct ActorScheduler {
    actors: HashMap<ActorId, BoxedActorRef>,
}

impl ActorScheduler {
    pub fn new() -> ActorRef<ActorScheduler>  {
        start_actor(ActorScheduler {
            actors: HashMap::new()
        })
    }
}

impl ActorRef<ActorScheduler> {

}
