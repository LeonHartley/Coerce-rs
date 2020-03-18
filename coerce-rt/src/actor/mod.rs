use crate::actor::context::{ActorContext, ActorHandlerContext, ActorStatus};
use crate::actor::lifecycle::{Status, Stop};
use crate::actor::message::{ActorMessage, Exec, Handler, Message, MessageHandler};
use log::error;
use std::any::Any;
use uuid::Uuid;

pub mod context;
pub mod lifecycle;
pub mod message;
pub mod scheduler;

pub type ActorId = String;

#[async_trait]
pub trait Actor {
    async fn started(&mut self, _ctx: &mut ActorHandlerContext) {}

    async fn stopped(&mut self, _ctx: &mut ActorHandlerContext) {}
}

pub trait GetActorRef {
    fn get_ref(&self, ctx: &ActorHandlerContext) -> ActorRef<Self>
    where
        Self: Actor + Sized + Send + Sync;
}

impl<A: Actor> GetActorRef for A {
    fn get_ref(&self, ctx: &ActorHandlerContext) -> ActorRef<Self>
    where
        Self: Sized + Send + Sync,
    {
        ctx.actor_ref()
    }
}

pub async fn new_actor<A: Actor>(actor: A) -> Result<ActorRef<A>, ActorRefErr>
where
    A: 'static + Sync + Send,
{
    ActorContext::current_context()
        .new_tracked_actor(actor)
        .await
}

pub async fn get_actor<A: Actor>(id: ActorId) -> Option<ActorRef<A>>
where
    A: 'static + Sync + Send,
{
    ActorContext::current_context().get_tracked_actor(id).await
}

#[derive(Clone)]
pub struct BoxedActorRef {
    id: ActorId,
    sender: tokio::sync::mpsc::Sender<Box<dyn Any + Sync + Send>>,
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
pub enum ActorRefErr {
    ActorUnavailable,
}

impl std::fmt::Display for ActorRefErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ActorRefErr::ActorUnavailable => write!(f, "actor unavailable"),
        }
    }
}

impl std::error::Error for ActorRefErr {}

impl<A: Actor> ActorRef<A>
where
    A: Sync + Send + 'static,
{
    pub async fn send<Msg: Message>(&mut self, msg: Msg) -> Result<Msg::Result, ActorRefErr>
    where
        Msg: 'static + Send + Sync,
        A: Handler<Msg>,
        Msg::Result: Send + Sync,
    {
        let (tx, rx) = tokio::sync::oneshot::channel();
        match self
            .sender
            .send(Box::new(ActorMessage::new(msg, Some(tx))))
            .await
        {
            Ok(_) => match rx.await {
                Ok(res) => Ok(res),
                Err(_e) => {
                    error!(target: "ActorRef", "error receiving result");
                    Err(ActorRefErr::ActorUnavailable)
                }
            },
            Err(_e) => {
                error!(target: "ActorRef", "error sending message");
                Err(ActorRefErr::ActorUnavailable)
            }
        }
    }

    pub async fn notify<Msg: Message>(&mut self, msg: Msg) -> Result<(), ActorRefErr>
    where
        Msg: 'static + Send + Sync,
        A: Handler<Msg>,
        Msg::Result: Send + Sync,
    {
        match self
            .sender
            .send(Box::new(ActorMessage::new(msg, None)))
            .await
        {
            Ok(_) => Ok(()),
            Err(_e) => Err(ActorRefErr::ActorUnavailable),
        }
    }

    pub async fn exec<F, R>(&mut self, f: F) -> Result<R, ActorRefErr>
    where
        F: (FnMut(&mut A) -> R) + 'static + Send + Sync,
        R: 'static + Send + Sync,
    {
        self.send(Exec::new(f)).await
    }

    pub async fn notify_exec<F>(&mut self, f: F) -> Result<(), ActorRefErr>
    where
        F: (FnMut(&mut A) -> ()) + 'static + Send + Sync,
    {
        self.notify(Exec::new(f)).await
    }

    pub async fn status(&mut self) -> Result<ActorStatus, ActorRefErr> {
        self.send(Status {}).await
    }

    pub async fn stop(&mut self) -> Result<ActorStatus, ActorRefErr> {
        self.send(Stop {}).await
    }
}
