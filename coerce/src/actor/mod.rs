use crate::actor::context::{ActorContext, ActorStatus};
use crate::actor::lifecycle::{Status, Stop};
use crate::actor::message::{ActorMessage, EnvelopeType, Exec, Handler, Message, MessageHandler};
use crate::actor::system::ActorSystem;
use crate::remote::RemoteActorRef;
use log::error;

use std::any::Any;
use uuid::Uuid;

pub mod context;
pub mod lifecycle;
pub mod message;
pub mod scheduler;
pub mod system;

pub type ActorId = String;

#[async_trait]
pub trait Actor: 'static + Send + Sync {
    async fn started(&mut self, _ctx: &mut ActorContext) {}

    async fn stopped(&mut self, _ctx: &mut ActorContext) {}
}

pub enum ActorCreationErr {
    InvalidRecipe(String),
}

pub trait ActorRecipe: Sized {
    fn read_from_bytes(bytes: Vec<u8>) -> Option<Self>;

    fn write_to_bytes(&self) -> Option<Vec<u8>>;
}

#[async_trait]
pub trait Factory: Clone {
    type Actor: Actor + 'static + Sync + Send;
    type Recipe: ActorRecipe + 'static + Sync + Send;

    async fn create(&self, recipe: Self::Recipe) -> Result<Self::Actor, ActorCreationErr>;
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct ActorState {
    pub actor_id: ActorId,
    pub state: Vec<u8>,
}

pub trait GetActorRef {
    fn get_ref(&self, ctx: &ActorContext) -> LocalActorRef<Self>
    where
        Self: Actor + Sized + Send + Sync;
}

impl<A: Actor> GetActorRef for A {
    fn get_ref(&self, ctx: &ActorContext) -> LocalActorRef<Self>
    where
        Self: Sized + Send + Sync,
    {
        ctx.actor_ref()
    }
}

pub async fn new_actor<A: Actor>(actor: A) -> Result<LocalActorRef<A>, ActorRefErr>
where
    A: 'static + Sync + Send,
{
    ActorSystem::current_system().new_tracked_actor(actor).await
}

pub async fn get_actor<A: Actor>(id: ActorId) -> Option<LocalActorRef<A>>
where
    A: 'static + Sync + Send,
{
    ActorSystem::current_system().get_tracked_actor(id).await
}

pub(crate) enum Ref<A: Actor>
where
    A: 'static + Sync + Send,
{
    Local(LocalActorRef<A>),
    Remote(RemoteActorRef<A>),
}

pub struct ActorRef<A: Actor>
where
    A: 'static + Sync + Send,
{
    inner_ref: Ref<A>,
}

impl<A: Actor> ActorRef<A>
where
    A: 'static + Sync + Send,
{
    pub async fn send<Msg: Message>(&mut self, msg: Msg) -> Result<Msg::Result, ActorRefErr>
    where
        Msg: 'static + Sync + Send,
        Msg::Result: 'static + Sync + Send,
        A: Handler<Msg>,
    {
        match &mut self.inner_ref {
            Ref::Local(local_ref) => local_ref.send(msg).await,
            Ref::Remote(remote_ref) => {
                remote_ref
                    .send(msg.into_envelope(EnvelopeType::Remote).unwrap())
                    .await
            }
        }
    }

    pub fn is_local(&self) -> bool {
        match &self.inner_ref {
            &Ref::Local(_) => true,
            _ => false,
        }
    }

    pub fn is_remote(&self) -> bool {
        match &self.inner_ref {
            &Ref::Remote(_) => true,
            _ => false,
        }
    }
}

impl<A: 'static + Actor + Sync + Send> From<LocalActorRef<A>> for ActorRef<A> {
    fn from(r: LocalActorRef<A>) -> Self {
        ActorRef {
            inner_ref: Ref::Local(r),
        }
    }
}

impl<A: 'static + Actor + Sync + Send> From<RemoteActorRef<A>> for ActorRef<A> {
    fn from(r: RemoteActorRef<A>) -> Self {
        ActorRef {
            inner_ref: Ref::Remote(r),
        }
    }
}

#[derive(Clone)]
pub struct BoxedActorRef {
    id: ActorId,
    system_id: Option<Uuid>,
    sender: tokio::sync::mpsc::UnboundedSender<Box<dyn Any + Sync + Send>>,
}

pub struct LocalActorRef<A: Actor>
where
    A: 'static + Send + Sync,
{
    pub id: ActorId,
    pub system_id: Option<Uuid>,
    sender: tokio::sync::mpsc::UnboundedSender<MessageHandler<A>>,
}

impl<A: Actor> From<BoxedActorRef> for LocalActorRef<A>
where
    A: 'static + Send + Sync,
{
    fn from(b: BoxedActorRef) -> Self {
        LocalActorRef {
            id: b.id,
            system_id: b.system_id,
            sender: unsafe { std::mem::transmute(b.sender) },
        }
    }
}

impl<A: Actor> From<LocalActorRef<A>> for BoxedActorRef
where
    A: 'static + Send + Sync,
{
    fn from(r: LocalActorRef<A>) -> Self {
        BoxedActorRef {
            system_id: r.system_id,
            id: r.id,
            sender: unsafe { std::mem::transmute(r.sender) },
        }
    }
}

impl<A> Clone for LocalActorRef<A>
where
    A: Actor + Sync + Send + 'static,
{
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            system_id: self.system_id,
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

impl<A: Actor> LocalActorRef<A>
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
        match self.sender.send(Box::new(ActorMessage::new(msg, Some(tx)))) {
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

    pub fn notify<Msg: Message>(&mut self, msg: Msg) -> Result<(), ActorRefErr>
    where
        Msg: 'static + Send + Sync,
        A: Handler<Msg>,
        Msg::Result: Send + Sync,
    {
        match self.sender.send(Box::new(ActorMessage::new(msg, None))) {
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

    pub fn notify_exec<F>(&mut self, f: F) -> Result<(), ActorRefErr>
    where
        F: (FnMut(&mut A) -> ()) + 'static + Send + Sync,
    {
        self.notify(Exec::new(f))
    }

    pub async fn status(&mut self) -> Result<ActorStatus, ActorRefErr> {
        self.send(Status {}).await
    }

    pub async fn stop(&mut self) -> Result<ActorStatus, ActorRefErr> {
        self.send(Stop {}).await
    }
}

pub fn new_actor_id() -> ActorId {
    Uuid::new_v4().to_string()
}
