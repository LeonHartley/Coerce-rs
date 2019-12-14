use crate::actor::context::{ActorContext, ActorStatus};
use crate::actor::lifecycle::{actor_loop, Stop};
use crate::actor::message::{
    ActorMessage, ActorMessageHandler, Exec, Handler, Message, MessageHandler, MessageResult,
};
use crate::actor::{Actor, ActorId};
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
        let (tx, rx) = tokio::sync::mpsc::channel(100);

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

#[derive(Clone)]
pub struct BoxedActorRef {
    id: Uuid,
    sender: tokio::sync::mpsc::Sender<Box<dyn Any>>,
}

pub struct ActorRef<A: Actor>
where
    A: 'static + Send + Sync,
{
    pub id: ActorId,
    sender: tokio::sync::mpsc::Sender<MessageHandler<A>>,
}

impl<A: Actor> From<BoxedActorRef> for ActorRef<A>
where
    A: 'static + Send + Sync,
{
    fn from(b: BoxedActorRef) -> Self {
        ActorRef {
            id: b.id,
            sender: unsafe { std::mem::transmute(b.sender) },
        }
    }
}

impl<A: Actor> From<ActorRef<A>> for BoxedActorRef
where
    A: 'static + Send + Sync,
{
    fn from(r: ActorRef<A>) -> Self {
        BoxedActorRef {
            id: r.id,
            sender: unsafe { std::mem::transmute(r.sender) },
        }
    }
}

impl<A> Clone for ActorRef<A>
where
    A: Actor + Sync + Send + 'static,
{
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            sender: self.sender.clone(),
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum ActorRefError {
    ActorUnavailable,
}

impl<A: Actor> ActorRef<A>
where
    A: Sync + Send + 'static,
{
    pub async fn send<Msg: Message>(&mut self, msg: Msg) -> Result<Msg::Result, ActorRefError>
    where
        Msg: 'static + Send + Sync,
        A: Handler<Msg>,
        Msg::Result: Send + Sync,
    {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.sender.send(Box::new(ActorMessage::new(msg, tx))).await;

        match rx.await {
            Ok(res) => Ok(res),
            Err(e) => {
                println!("{:?}", e);
                Err(ActorRefError::ActorUnavailable)
            }
        }
    }

    pub async fn exec<F, R>(&mut self, f: F) -> Result<R, ActorRefError>
    where
        F: (FnMut(&mut A) -> R) + 'static + Send + Sync,
        R: 'static + Send + Sync,
    {
        self.send(Exec::new(f)).await
    }

    pub async fn stop(&mut self) -> Result<ActorStatus, ActorRefError> {
        self.send(Stop {}).await
    }
}
