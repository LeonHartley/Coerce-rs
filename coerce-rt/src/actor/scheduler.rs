use crate::actor::context::ActorStatus::{Started, Starting, Stopped, Stopping};
use crate::actor::context::{ActorContext, ActorHandlerContext, ActorStatus};
use crate::actor::lifecycle::{actor_loop, Stop};
use crate::actor::message::{ActorMessage, ActorMessageHandler, Handler, Message, MessageResult};
use crate::actor::{Actor, ActorId};
use std::any::{Any, TypeId};
use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::mem::transmute;
use std::ops::DerefMut;
use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

pub struct ActorScheduler {
    actors: HashMap<ActorId, BoxedActorRef>,
}

pub type MessageHandler<A> = Box<dyn ActorMessageHandler<A> + Sync + Send>;

impl ActorScheduler {
    pub fn new() -> ActorScheduler {
        ActorScheduler {
            actors: HashMap::new(),
        }
    }

    pub fn register<A: Actor + Sync + Send>(
        &mut self,
        mut actor: A,
        ctx: Arc<Mutex<ActorContext>>,
    ) -> ActorRef<A> {
        let id = ActorId::new_v4();
        let (mut tx, mut rx) = tokio::sync::mpsc::channel(100);

        let actor_ref = ActorRef {
            id: id.clone(),
            sender: tx.clone(),
        };

        let boxed_ref = BoxedActorRef {
            id,
            sender: unsafe { std::mem::transmute(tx.clone()) },
        };

        tokio::spawn(actor_loop(actor, rx));

        actor_ref
    }

    pub fn run<F, A: Actor>(actor: &mut A, callback: &mut F)
    where
        F: (FnMut(&mut A) -> ()),
    {
        callback(actor)
    }
}

pub struct BoxedActorRef {
    id: Uuid,
    sender: tokio::sync::mpsc::Sender<Box<dyn Any>>,
}

pub struct ActorRef<A: Actor>
where
    A: 'static + Send + Sync,
{
    id: Uuid,
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
        let (mut tx, rx) = tokio::sync::oneshot::channel();
        self.sender.send(Box::new(ActorMessage::new(msg, tx))).await;

        match rx.await {
            Ok(res) => Ok(res),
            Err(e) => {
                println!("{:?}", e);
                Err(ActorRefError::ActorUnavailable)
            }
        }
    }

    pub async fn stop(&mut self) -> Result<ActorStatus, ActorRefError> {
        self.send(Stop {}).await
    }
}
