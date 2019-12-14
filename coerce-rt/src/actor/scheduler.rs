use crate::actor::context::{ActorContext, ActorStatus};
use crate::actor::lifecycle::{actor_loop, Status, Stop};
use crate::actor::message::{
    ActorMessage, ActorMessageHandler, Exec, Handler, Message, MessageHandler, MessageResult,
};
use crate::actor::{Actor, ActorId, BoxedActorRef, ActorRef};
use std::any::Any;

use std::collections::HashMap;

use std::sync::{Arc, Mutex};
use uuid::Uuid;

pub struct ActorScheduler {
    actors: HashMap<ActorId, BoxedActorRef>,
}

impl ActorScheduler {
    pub fn new() -> ActorScheduler {
        ActorScheduler {
            actors: HashMap::new(),
        }
    }

    pub fn register<A: Actor>(&mut self, actor: A, _ctx: Arc<Mutex<ActorContext>>) -> ActorRef<A>
    where
        A: 'static + Sync + Send,
    {
        let id = ActorId::new_v4();
        let (tx, rx) = tokio::sync::mpsc::channel(128);

        let actor_ref = ActorRef {
            id: id.clone(),
            sender: tx.clone(),
        };

        let _ = self
            .actors
            .insert(id, BoxedActorRef::from(actor_ref.clone()));
        tokio::spawn(actor_loop(actor, rx));

        actor_ref
    }

    pub fn get_actor<A: Actor>(&mut self, id: &ActorId) -> Option<ActorRef<A>>
    where
        A: 'static + Sync + Send,
    {
        match self.actors.get(id) {
            Some(actor) => Some(ActorRef::<A>::from(actor.clone())),
            None => None,
        }
    }
}
