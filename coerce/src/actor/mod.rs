use crate::actor::context::{ActorContext, ActorStatus};
use crate::actor::lifecycle::{Status, Stop};
use crate::actor::message::{ActorMessage, EnvelopeType, Exec, Handler, Message, MessageHandler};
use crate::actor::system::ActorSystem;
use crate::remote::RemoteActorRef;

use crate::actor::scheduler::ActorType::Tracked;
use std::any::Any;

use crate::actor::supervised::Terminated;
use futures::SinkExt;
use std::sync::Arc;
use uuid::Uuid;

pub mod context;
pub mod lifecycle;
pub mod message;
pub mod scheduler;
pub mod supervised;
pub mod system;
pub mod worker;

pub type ActorId = String;

#[async_trait]
pub trait Actor: 'static + Send + Sync {
    fn new_context(
        system: Option<ActorSystem>,
        status: ActorStatus,
        boxed_ref: BoxedActorRef,
    ) -> ActorContext
    where
        Self: Sized,
    {
        ActorContext::new(system, status, boxed_ref)
    }

    async fn started(&mut self, _ctx: &mut ActorContext) {}

    async fn stopped(&mut self, _ctx: &mut ActorContext) {}

    async fn on_child_stopped(&mut self, id: &ActorId, ctx: &mut ActorContext) {}

    fn type_name() -> &'static str
    where
        Self: Sized,
    {
        std::any::type_name::<Self>()
    }
}

#[async_trait]
pub trait IntoActor: Actor + Sized {
    async fn into_actor(
        self,
        id: Option<ActorId>,
        sys: &ActorSystem,
    ) -> Result<LocalActorRef<Self>, ActorRefErr>;
}

#[async_trait]
pub trait IntoChild: Actor + Sized {
    async fn into_child(
        self,
        id: Option<ActorId>,
        ctx: &mut ActorContext,
    ) -> Result<LocalActorRef<Self>, ActorRefErr>;
}

pub enum ActorCreationErr {
    InvalidRecipe(String),
}

pub trait ActorRecipe: Sized {
    fn read_from_bytes(bytes: Vec<u8>) -> Option<Self>;

    fn write_to_bytes(&self) -> Option<Vec<u8>>;
}

#[async_trait]
pub trait ActorFactory: Clone {
    type Actor: Actor + 'static + Sync + Send;
    type Recipe: ActorRecipe + 'static + Sync + Send;

    async fn create(&self, recipe: Self::Recipe) -> Result<Self::Actor, ActorCreationErr>;
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

pub(crate) enum Ref<A: Actor> {
    Local(LocalActorRef<A>),
    Remote(RemoteActorRef<A>),
}

pub struct ActorRef<A: Actor>
where
    A: 'static + Sync + Send,
{
    inner_ref: Ref<A>,
}

#[async_trait]
trait MessageReceiver<M: Message>: 'static + Send + Sync {
    async fn send(&self, msg: M) -> Result<M::Result, ActorRefErr>;
}

#[async_trait]
impl<A: Actor, M: Message> MessageReceiver<M> for LocalActorRef<A>
where
    A: Handler<M>,
{
    async fn send(&self, msg: M) -> Result<M::Result, ActorRefErr> {
        self.send(msg).await
    }
}

impl<A: Actor, M: Message> From<LocalActorRef<A>> for Receiver<M>
where
    A: Handler<M>,
{
    fn from(actor_ref: LocalActorRef<A>) -> Self {
        Self(Box::new(actor_ref))
    }
}

pub struct Receiver<M: Message>(Box<dyn MessageReceiver<M>>);

impl<M: Message> Receiver<M> {
    pub async fn send(&self, msg: M) -> Result<M::Result, ActorRefErr> {
        self.0.send(msg).await
    }
}

impl<A: Actor> ActorRef<A> {
    pub async fn send<Msg: Message>(&self, msg: Msg) -> Result<Msg::Result, ActorRefErr>
    where
        A: Handler<Msg>,
    {
        match &self.inner_ref {
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

#[async_trait]
impl<A: Actor> IntoActor for A {
    async fn into_actor(
        self,
        id: Option<ActorId>,
        sys: &ActorSystem,
    ) -> Result<LocalActorRef<Self>, ActorRefErr> {
        if let Some(id) = id {
            sys.new_actor(id, self, Tracked).await
        } else {
            sys.new_anon_actor(self).await
        }
    }
}

impl<A: Actor> From<LocalActorRef<A>> for ActorRef<A> {
    fn from(r: LocalActorRef<A>) -> Self {
        ActorRef {
            inner_ref: Ref::Local(r),
        }
    }
}

impl<A: Actor> From<RemoteActorRef<A>> for ActorRef<A> {
    fn from(r: RemoteActorRef<A>) -> Self {
        ActorRef {
            inner_ref: Ref::Remote(r),
        }
    }
}

#[async_trait]
pub trait CoreActorRef: Any {
    fn actor_id(&self) -> &ActorId;

    async fn status(&self) -> Result<ActorStatus, ActorRefErr>;

    async fn stop(&self) -> Result<ActorStatus, ActorRefErr>;

    fn notify_stop(&self) -> Result<(), ActorRefErr>;

    fn notify_child_terminated(&self, id: ActorId) -> Result<(), ActorRefErr>;

    fn as_any(&self) -> &dyn Any;
}

#[derive(Clone)]
pub struct BoxedActorRef(Arc<dyn CoreActorRef + Send + Sync>);

pub struct LocalActorRef<A: Actor> {
    pub id: ActorId,
    pub system_id: Option<Uuid>,
    sender: tokio::sync::mpsc::UnboundedSender<MessageHandler<A>>,
}

impl<A: Actor> From<LocalActorRef<A>> for BoxedActorRef {
    fn from(r: LocalActorRef<A>) -> Self {
        BoxedActorRef(Arc::new(r))
    }
}

impl<A: Actor> Clone for LocalActorRef<A> {
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
    AlreadyExists(ActorId),
}

impl std::fmt::Display for ActorRefErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ActorRefErr::ActorUnavailable => write!(f, "actor unavailable"),
            ActorRefErr::AlreadyExists(id) => write!(f, "actor {} already exists", &id),
        }
    }
}

impl std::error::Error for ActorRefErr {}

impl<A: Actor> LocalActorRef<A> {
    pub async fn send<Msg: Message>(&self, msg: Msg) -> Result<Msg::Result, ActorRefErr>
    where
        A: Handler<Msg>,
    {
        let message_type = msg.name();
        let actor_type = A::type_name();
        let span = tracing::trace_span!("LocalActorRef::send", actor_type, message_type);
        let _enter = span.enter();

        let (tx, rx) = tokio::sync::oneshot::channel();
        match self.sender.send(Box::new(ActorMessage::new(msg, Some(tx)))) {
            Ok(_) => match rx.await {
                Ok(res) => {
                    tracing::trace!("recv result");
                    Ok(res)
                }
                Err(e) => Err(ActorRefErr::ActorUnavailable),
            },
            Err(_e) => Err(ActorRefErr::ActorUnavailable),
        }
    }

    pub fn notify<Msg: Message>(&self, msg: Msg) -> Result<(), ActorRefErr>
    where
        A: Handler<Msg>,
    {
        let message_type = msg.name();
        let span = tracing::span!(
            tracing::Level::TRACE,
            "LocalActorRef::notify",
            message_type = message_type
        );

        let _enter = span.enter();

        match self.sender.send(Box::new(ActorMessage::new(msg, None))) {
            Ok(_) => Ok(()),
            Err(_e) => Err(ActorRefErr::ActorUnavailable),
        }
    }

    pub async fn exec<F, R>(&self, f: F) -> Result<R, ActorRefErr>
    where
        F: (FnMut(&mut A) -> R) + 'static + Send + Sync,
        R: 'static + Send + Sync,
    {
        self.send(Exec::new(f)).await
    }

    pub fn notify_exec<F>(&self, f: F) -> Result<(), ActorRefErr>
    where
        F: (FnMut(&mut A) -> ()) + 'static + Send + Sync,
    {
        self.notify(Exec::new(f))
    }

    pub fn actor_id(&self) -> &ActorId {
        &self.id
    }

    pub async fn status(&self) -> Result<ActorStatus, ActorRefErr> {
        self.send(Status {}).await
    }

    pub async fn stop(&self) -> Result<ActorStatus, ActorRefErr> {
        self.send(Stop {}).await
    }

    pub fn notify_stop(&self) -> Result<(), ActorRefErr> {
        self.notify(Stop {})
    }
}

#[async_trait]
impl<A: Actor> CoreActorRef for LocalActorRef<A> {
    fn actor_id(&self) -> &ActorId {
        self.actor_id()
    }

    async fn status(&self) -> Result<ActorStatus, ActorRefErr> {
        self.status().await
    }

    async fn stop(&self) -> Result<ActorStatus, ActorRefErr> {
        self.stop().await
    }

    fn notify_stop(&self) -> Result<(), ActorRefErr> {
        self.notify_stop()
    }

    fn notify_child_terminated(&self, id: ActorId) -> Result<(), ActorRefErr> {
        self.notify(Terminated(id))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[async_trait]
impl CoreActorRef for BoxedActorRef {
    fn actor_id(&self) -> &ActorId {
        self.0.actor_id()
    }

    async fn status(&self) -> Result<ActorStatus, ActorRefErr> {
        self.0.status().await
    }

    async fn stop(&self) -> Result<ActorStatus, ActorRefErr> {
        self.0.stop().await
    }

    fn notify_stop(&self) -> Result<(), ActorRefErr> {
        self.0.notify_stop()
    }

    fn notify_child_terminated(&self, id: ActorId) -> Result<(), ActorRefErr> {
        self.0.notify_child_terminated(id)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub fn new_actor_id() -> ActorId {
    Uuid::new_v4().to_string()
}
