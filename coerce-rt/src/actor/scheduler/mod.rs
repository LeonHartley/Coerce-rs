use crate::actor::context::{ActorContext, ActorStatus, ActorHandlerContext};
use crate::actor::lifecycle::{actor_loop, Status, Stop};
use crate::actor::message::{
    ActorMessage, ActorMessageHandler, Exec, Handler, Message, MessageHandler, MessageResult,
};
use crate::actor::{Actor, ActorId, ActorRef, BoxedActorRef};
use std::any::Any;

use std::collections::HashMap;

use std::sync::{Arc, Mutex};
use uuid::Uuid;

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

#[async_trait]
impl Actor for ActorScheduler {}

pub struct RegisterActor<A: Actor>(pub A) where A: 'static + Sync + Send;

impl<A: Actor> Message for RegisterActor<A> where A: 'static + Sync + Send {
    type Result = ActorRef<A>;
}

#[async_trait]
impl<A: Actor> Handler<RegisterActor<A>> for ActorScheduler where A: 'static + Sync + Send {
    async fn handle(&mut self, message: RegisterActor<A>, ctx: &mut ActorHandlerContext) -> ActorRef<A>
    {
        let actor = start_actor(message.0);

        let _ = self
            .actors
            .insert(actor.id, BoxedActorRef::from(actor.clone()));

        actor
    }
}

pub fn start_actor<A: Actor>(actor: A) -> ActorRef<A> where A: 'static + Send + Sync {
    let id = ActorId::new_v4();
    let (tx, rx) = tokio::sync::mpsc::channel(128);

    tokio::spawn(actor_loop(actor, rx));

    ActorRef {
        id: id.clone(),
        sender: tx.clone(),
    }
}
