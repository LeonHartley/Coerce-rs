//! Coerce Actor Runtime
//!
//! An [`Actor`], at a high level, is a primitive that describes a unit of computation.
//! Internally, it can have mutable state, receive messages and perform actions. The only way
//! to communicate with an actor is by delivering messages to its mailbox.
//!
//! Actors can only process one message at a time, this can be useful because it can alleviate the
//! need for thread synchronisation, usually achieved by locking (using Mutex, RwLock etc).
//!
//! ## How is this achieved in Coerce?
//! Under the hood, Coerce uses [tokio]'s [mpsc] and [oneshot] channels for message communication.
//! When an actor is created, an [mpsc] channel is created, used as the [`Actor`]'s mailbox.
//! A task is spawned, listening for messages from a receiver, handling and emitting the result
//! of the message to the sender's [oneshot] channel (if applicable).
//!
//! Every reference ([`LocalActorRef`]) holds a Sender<M> where [`Actor`]: [`Handler<M>`],
//! which can be cheaply cloned.
//!
//! Whilst message handlers are `async`, the actor will always wait until for handler completion
//! before moving onto subsequent messages in the mailbox. If the actor needs to defer work and
//! return a result faster, an asynchronous task should be spawned.
//!
//! ## General lifecycle of an [`Actor`][Actor]:
//! 1. [`ActorContext`] is created
//! 2. Actor started ([`Actor::started`])
//! 3. Actor begins processing messages
//! 4. Actor reference registered with the [`ActorScheduler`] (if actor is [`ActorType::Tracked`])
//! 5. Actor reference sent back to the creator
//! 6. Stop request is received, or system is shutting down
//! 7. Actor stopping ([`Actor::stopped`])
//! 8. Actor reference deregistered from the [`ActorScheduler`] (if actor is [`ActorType::Tracked`])
//!
//! [tokio]: https://github.com/tokio/tokio-rs
//! [mpsc]: https://docs.rs/tokio/latest/tokio/sync/mpsc
//! [oneshot]: https://docs.rs/tokio/latest/tokio/sync/oneshot
//! [`LocalActorRef`]: LocalActorRef
//! [`Actor`]: Actor
//! [`Handler<M>`]: message::Handler
//! [`ActorContext`]: context::ActorContext
//! [`ActorScheduler`]: scheduler::ActorScheduler
//! [`ActorType::Tracked`]: scheduler::ActorType::Tracked
//! [`Actor::started`]: Actor::started
//! [`Actor::stopped`]: Actor::stopped
//!
//! ## Actor Example
//! The below example demonstrates how to create an actor that spawns {n} child actors, waits for them
//! to do work and then finally stops once completed.
//!```rust
//! use coerce::actor::{ActorId, IntoActor, IntoActorId, Actor};
//! use coerce::actor::context::ActorContext;
//! use coerce::actor::message::{Message, Handler};
//! use coerce::actor::system::ActorSystem;
//!
//! use async_trait::async_trait;
//! use tokio::sync::oneshot::{channel, Sender};
//!
//! struct ParentActor {
//!    child_count: usize,
//!    completed_actors: usize,
//!    on_work_completed: Option<Sender<usize>>,
//! }
//!
//! struct ChildActor;
//!
//! #[tokio::main]
//! pub async fn main() {
//!    let system = ActorSystem::new();
//!    let (tx, rx) = channel();
//!
//!    const TOTAL_CHILD_ACTORS: usize = 10;
//!
//!    let actor = ParentActor {
//!            child_count: TOTAL_CHILD_ACTORS,
//!            completed_actors: 0,
//!            on_work_completed: Some(tx),
//!         }
//!        .into_actor(Some("parent"), &system)
//!        .await
//!        .unwrap();
//!
//!    let completed_actors = rx.await.ok();
//!    assert_eq!(completed_actors, Some(TOTAL_CHILD_ACTORS))
//! }
//!
//! #[async_trait]
//! impl Actor for ParentActor {
//!    async fn started(&mut self, ctx: &mut ActorContext) {
//!        for i in 0..self.child_count {
//!            ctx.spawn(format!("child-{}", i).into_actor_id(), ChildActor).await.unwrap();
//!        }
//!    }
//!
//!    async fn on_child_stopped(&mut self, id: &ActorId, ctx: &mut ActorContext) {
//!        println!("child actor (id={}) stopped", ctx.id());
//!
//!        self.completed_actors += 1;
//!
//!        if ctx.supervised_count() == 0 && self.completed_actors == self.child_count {
//!            println!("all child actors finished, stopping ParentActor");
//!
//!            if let Some(on_work_completed) = self.on_work_completed.take() {
//!                let _ = on_work_completed.send(self.completed_actors);
//!            }
//!
//!            ctx.stop(None);
//!        }
//!    }
//! }
//!
//! struct Finished;
//!
//! impl Message for Finished {
//!    type Result = ();
//! }
//!
//! #[async_trait]
//! impl Actor for ChildActor {
//!    async fn started(&mut self, ctx: &mut ActorContext) {
//!        println!("child actor (id={}) running", ctx.id());
//!
//!        // simulate some work that takes 5 milliseconds
//!        let _ = self.actor_ref(ctx)
//!                    .scheduled_notify(Finished, std::time::Duration::from_millis(5));
//!    }
//!
//!    async fn stopped(&mut self, ctx: &mut ActorContext) {
//!        println!("child actor (id={}) finished", ctx.id());
//!    }
//! }
//!
//! #[async_trait]
//! impl Handler<Finished> for ChildActor {
//!     async fn handle(&mut self, message: Finished, ctx: &mut ActorContext)  {
//!        ctx.stop(None);
//!     }
//! }
//!
//! ```
//!
use crate::actor::context::{ActorContext, ActorStatus};
use crate::actor::describe::Describe;
use crate::actor::lifecycle::{Status, Stop};
use crate::actor::message::{
    ActorMessage, Exec, Handler, Message, MessageHandler, MessageUnwrapErr, MessageWrapErr,
};
use crate::actor::metrics::ActorMetrics;
use crate::actor::scheduler::ActorType::{Anonymous, Tracked};
use crate::actor::supervised::Terminated;
use crate::actor::system::ActorSystem;
use std::any::Any;
use std::fmt::{Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use uuid::Uuid;

#[cfg(feature = "remote")]
use crate::actor::message::Envelope;

#[cfg(feature = "remote")]
use crate::remote::{system::NodeId, RemoteActorRef};

pub use refs::*;

pub mod blocking;
pub mod context;
pub mod describe;
// pub mod event;
pub mod lifecycle;
pub mod message;
pub mod metrics;
pub mod refs;
pub mod scheduler;
pub mod supervised;
pub mod system;
pub mod worker;

/// A reference to a string-based `ActorId`
pub type ActorId = Arc<str>;

/// A reference to a string-based `ActorPath`
pub type ActorPath = Arc<str>;

/// Actor definition, with specific lifecycle hooks.
#[async_trait]
pub trait Actor: 'static + Send + Sync {
    /// Creates a new [`ActorContext`], allowing actor implementations to override
    /// specific parts of the [`ActorContext`], before the actor is started.
    ///
    /// [`ActorContext`]: context::ActorContext
    fn new_context(
        &self,
        system: Option<ActorSystem>,
        status: ActorStatus,
        boxed_ref: BoxedActorRef,
    ) -> ActorContext {
        ActorContext::new(system, status, boxed_ref)
    }

    /// Called once the Actor has been started
    async fn started(&mut self, _ctx: &mut ActorContext) {}

    /// Called once the Actor has stopped
    async fn stopped(&mut self, _ctx: &mut ActorContext) {}

    /// Called when a supervised actor has stopped
    async fn on_child_stopped(&mut self, _id: &ActorId, _ctx: &mut ActorContext) {}

    /// Returns a [`LocalActorRef<Self>`] instance of the current actor,
    /// automatically casting from the [`ActorContext`][context::ActorContext]'s [`BoxedActorRef`][BoxedActorRef].
    ///
    /// [`LocalActorRef<Self>`]: LocalActorRef
    /// [`ActorContext`]: context::ActorContext
    /// [`BoxedActorRef`]: BoxedActorRef,
    fn actor_ref(&self, ctx: &ActorContext) -> LocalActorRef<Self>
    where
        Self: Sized,
    {
        ctx.actor_ref()
    }

    /// Returns the actor's type name string
    fn type_name() -> &'static str
    where
        Self: Sized,
    {
        std::any::type_name::<Self>()
    }
}

/// Trait allowing the creation of an [`Actor`][Actor] directly from itself
#[async_trait]
pub trait IntoActor: Actor + Sized {
    async fn into_actor<'a, I: 'a + IntoActorId + Send>(
        self,
        id: Option<I>,
        sys: &ActorSystem,
    ) -> Result<LocalActorRef<Self>, ActorRefErr>;

    async fn into_anon_actor<'a, I: 'a + IntoActorId + Send>(
        self,
        id: Option<I>,
        sys: &ActorSystem,
    ) -> Result<LocalActorRef<Self>, ActorRefErr>;
}

/// Trait allowing the creation of a supervised [`Actor`][Actor] directly from itself
#[async_trait]
pub trait IntoChild: Actor + Sized {
    async fn into_child(
        self,
        id: Option<ActorId>,
        ctx: &mut ActorContext,
    ) -> Result<LocalActorRef<Self>, ActorRefErr>;
}

/// Trait that defines how a specific [`Actor`] implementation can be created, allowing
/// things like distributed sharding and remoting to initialise [`Actor`]s from a
/// pre-defined [`ActorRecipe`].
///
/// [`Actor`]: Actor
/// [`ActorRecipe`]: ActorRecipe
#[async_trait]
pub trait ActorFactory: Clone {
    /// [`Actor`][Actor] type that is created by the [`ActorFactory`][ActorFactory]
    type Actor: Actor + 'static + Sync + Send;

    /// The [`ActorRecipe`][ActorRecipe] type to be used
    type Recipe: ActorRecipe + 'static + Sync + Send;

    /// Creates an [`Actor`][Actor] implementation from a provided [`ActorRecipe`][ActorRecipe]
    async fn create(&self, recipe: Self::Recipe) -> Result<Self::Actor, ActorCreationErr>;
}

/// The error returned when a factory has failed to create the requested actor.
pub enum ActorCreationErr {
    /// Recipe provided was invalid, possibly a de-serialisation issue.
    InvalidRecipe(String),
}

/// Trait that defines the arguments used to initialise specific [`Actor`][Actor] implementations
pub trait ActorRecipe: Sized {
    fn read_from_bytes(bytes: &Vec<u8>) -> Option<Self>;

    fn write_to_bytes(&self) -> Option<Vec<u8>>;
}

/// Default implementation for (), allowing actors to be created with no [`ActorRecipe`][ActorRecipe].
impl ActorRecipe for () {
    fn read_from_bytes(_: &Vec<u8>) -> Option<Self> {
        Some(())
    }

    fn write_to_bytes(&self) -> Option<Vec<u8>> {
        Some(vec![])
    }
}

/// Creates an actor using the global [`ActorSystem`][system::ActorSystem]
pub async fn new_actor<A: Actor>(actor: A) -> Result<LocalActorRef<A>, ActorRefErr>
where
    A: 'static + Sync + Send,
{
    ActorSystem::global_system().new_tracked_actor(actor).await
}

/// Gets an actor reference from the global [`ActorSystem`][system::ActorSystem]
pub async fn get_actor<A: Actor>(id: ActorId) -> Option<LocalActorRef<A>>
where
    A: 'static + Sync + Send,
{
    ActorSystem::global_system().get_tracked_actor(id).await
}

/// Trait allowing the type-omission of [`Actor`][Actor] types but still allowing
/// statically-typed message transmission
#[async_trait]
pub trait MessageReceiver<M: Message>: 'static + Send + Sync + MessageReceiverClone<M> {
    async fn send(&self, msg: M) -> Result<M::Result, ActorRefErr>;
    fn notify(&self, msg: M) -> Result<(), ActorRefErr>;
}

/// Trait helping us to clone [`MessageReceiver`][MessageReceiver]s trait-objects.
pub trait MessageReceiverClone<M: Message>: 'static + Send + Sync {
    fn clone_box(&self) -> Box<dyn MessageReceiver<M>>;
}

impl<T, M> MessageReceiverClone<M> for T
where
    T: 'static + MessageReceiver<M> + Clone,
    M: Message,
{
    fn clone_box(&self) -> Box<dyn MessageReceiver<M>> {
        Box::new(self.clone())
    }
}

impl<M: Message> Clone for Box<dyn MessageReceiver<M>> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

#[async_trait]
impl<A: Actor, M: Message> MessageReceiver<M> for LocalActorRef<A>
where
    A: Handler<M>,
{
    async fn send(&self, msg: M) -> Result<M::Result, ActorRefErr> {
        self.send(msg).await
    }

    fn notify(&self, msg: M) -> Result<(), ActorRefErr> {
        self.notify(msg)
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

/// Allows type-omission of [`Actor`][Actor] types but still allowing
/// statically-typed message transmission
#[derive(Clone)]
pub struct Receiver<M: Message>(Box<dyn MessageReceiver<M>>);

impl<M: Message> Receiver<M> {
    pub async fn send(&self, msg: M) -> Result<M::Result, ActorRefErr> {
        self.0.send(msg).await
    }

    pub fn notify(&self, msg: M) -> Result<(), ActorRefErr> {
        self.0.notify(msg)
    }
}

#[async_trait]
impl<A: Actor> IntoActor for A {
    async fn into_actor<'a, I: 'a + IntoActorId + Send>(
        self,
        id: Option<I>,
        sys: &ActorSystem,
    ) -> Result<LocalActorRef<Self>, ActorRefErr> {
        let id = id.map(|i| i.into_actor_id());
        if let Some(id) = id {
            sys.new_actor(id, self, Tracked).await
        } else {
            sys.new_anon_actor(self).await
        }
    }

    async fn into_anon_actor<'a, I: 'a + IntoActorId + Send>(
        self,
        id: Option<I>,
        sys: &ActorSystem,
    ) -> Result<LocalActorRef<Self>, ActorRefErr> {
        sys.new_actor(
            id.map_or_else(new_actor_id, |id| id.into_actor_id()),
            self,
            Anonymous,
        )
        .await
    }
}

#[async_trait]
impl<A: Actor> IntoChild for A {
    async fn into_child(
        self,
        id: Option<ActorId>,
        ctx: &mut ActorContext,
    ) -> Result<LocalActorRef<Self>, ActorRefErr> {
        ctx.spawn(id.unwrap_or_else(new_actor_id), self).await
    }
}

/// A handle to a scheduled notification, which can be created via [`scheduled_notify`].
///
/// [`scheduled_notify`]: LocalActorRef::scheduled_notify
pub struct ScheduledNotify<A: Actor, M: Message> {
    cancellation_token: CancellationToken,
    _a: PhantomData<A>,
    _m: PhantomData<M>,
}

impl<A: Actor, M: Message> ScheduledNotify<A, M> {
    pub(crate) fn new(cancellation_token: CancellationToken) -> Self {
        ScheduledNotify {
            cancellation_token,
            _a: PhantomData,
            _m: PhantomData,
        }
    }

    /// Cancels the scheduled notification
    pub fn cancel(&self) {
        self.cancellation_token.cancel();
    }
}

impl<A: Actor> LocalActorRef<A> {
    /// Spawns an asynchronous task that sleeps for the provided duration and finally sends
    /// the provided message to the target `Actor`.
    ///
    /// Returns a handle to the scheduled notification for cancellation.
    pub fn scheduled_notify<M: Message>(&self, message: M, delay: Duration) -> ScheduledNotify<A, M>
    where
        A: Handler<M>,
    {
        let actor_ref = self.clone();
        let cancellation_token = CancellationToken::new();
        let cancellation_token_clone = cancellation_token.clone();
        let _join_handle = tokio::spawn(async move {
            tokio::select! {
                _ = cancellation_token_clone.cancelled() => { }
                _ = tokio::time::sleep(delay) => {
                    let _ = actor_ref.notify(message);
                }
            }
        });

        ScheduledNotify::new(cancellation_token)
    }
}

/// A reference to a string-based `ActorTag`
pub type ActorTag = Box<str>;

/// An [`Actor`][Actor] can return self-defined tags, which can be retrieved
/// by sending a [`Describe`][describe::Describe] to the target [`Actor`][Actor]
/// or by calling [`describe()`][LocalActorRef::describe] on the target [`Actor`][Actor]'s reference.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ActorTags {
    None,
    Tag(ActorTag),
    Tags(Vec<ActorTag>),
}

impl From<&'static str> for ActorTags {
    fn from(value: &'static str) -> Self {
        Self::Tag(value.into())
    }
}

impl From<String> for ActorTags {
    fn from(value: String) -> Self {
        Self::Tag(value.into())
    }
}

impl From<Vec<&'static str>> for ActorTags {
    fn from(value: Vec<&'static str>) -> Self {
        Self::Tags(value.into_iter().map(|s| s.into()).collect())
    }
}

impl From<Vec<String>> for ActorTags {
    fn from(value: Vec<String>) -> Self {
        Self::Tags(value.into_iter().map(|s| s.into()).collect())
    }
}

/// Creates a new random [`ActorId`][ActorId]
pub fn new_actor_id() -> ActorId {
    Uuid::new_v4().into_actor_id()
}

/// Trait allowing the conversion of a type into [`ActorId`][ActorId], by consuming the input
pub trait IntoActorId {
    fn into_actor_id(self) -> ActorId;
}

///
/// Trait allowing the conversion of a type into [`ActorId`][ActorId], by borrowing the input
///
pub trait ToActorId {
    fn to_actor_id(&self) -> ActorId;
}

/// Trait allowing the conversion of a type into [`ActorPath`][ActorPath], by consuming the input
///
pub trait IntoActorPath {
    fn into_actor_path(self) -> ActorPath;
}

impl<T: ToString + Send + Sync> IntoActorId for T {
    fn into_actor_id(self) -> ActorId {
        self.to_string().into()
    }
}

impl<T: ToString + Send + Sync> ToActorId for T {
    fn to_actor_id(&self) -> ActorId {
        self.to_string().into()
    }
}

impl<T: ToString + Send + Sync> IntoActorPath for T {
    fn into_actor_path(self) -> ActorPath {
        self.to_string().into()
    }
}
