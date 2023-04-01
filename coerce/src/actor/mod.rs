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

pub mod blocking;
pub mod context;
pub mod describe;
// pub mod event;
pub mod lifecycle;
pub mod message;
pub mod metrics;
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

/// The error returned when a factory has failed to create the requested actor.
pub enum ActorCreationErr {
    /// Recipe provided was invalid, possibly a de-serialisation issue.
    InvalidRecipe(String),
}

impl<A: Actor> Deref for LocalActorRef<A> {
    type Target = LocalActorRefInner<A>;

    #[inline]
    fn deref(&self) -> &LocalActorRefInner<A> {
        self.inner.as_ref()
    }
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

pub(crate) enum Ref<A: Actor> {
    Local(LocalActorRef<A>),

    #[cfg(feature = "remote")]
    Remote(RemoteActorRef<A>),
}

impl<A: Actor> Clone for Ref<A> {
    fn clone(&self) -> Self {
        match &self {
            Ref::Local(a) => Ref::Local(a.clone()),

            #[cfg(feature = "remote")]
            Ref::Remote(a) => Ref::Remote(a.clone()),
        }
    }
}

/// Location-transparent reference to an [`Actor`][Actor].
///
/// Supported targets:
/// - [`LocalActorRef<A>`][LocalActorRef]
/// - [`RemoteActorRef<A>`][crate::remote::RemoteActorRef]
///
pub struct ActorRef<A: Actor> {
    inner_ref: Ref<A>,
}

impl<A: Actor> Debug for ActorRef<A> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.inner_ref {
            Ref::Local(r) => r.fmt(f),

            #[cfg(feature = "remote")]
            Ref::Remote(remote_ref) => remote_ref.fmt(f),
        }
    }
}

impl<A: Actor> Display for ActorRef<A> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.inner_ref {
            Ref::Local(local_ref) => local_ref.fmt(f),

            #[cfg(feature = "remote")]
            Ref::Remote(remote_ref) => remote_ref.fmt(f),
        }
    }
}

impl<A: Actor> ActorRef<A> {
    pub fn actor_id(&self) -> &ActorId {
        match &self.inner_ref {
            Ref::Local(a) => a.actor_id(),

            #[cfg(feature = "remote")]
            Ref::Remote(a) => a.actor_id(),
        }
    }

    #[cfg(feature = "remote")]
    pub fn node_id(&self) -> Option<NodeId> {
        match &self.inner_ref {
            Ref::Local(_) => None,
            Ref::Remote(r) => Some(r.node_id()),
        }
    }
}

impl<A: Actor> Clone for ActorRef<A> {
    fn clone(&self) -> Self {
        Self {
            inner_ref: self.inner_ref.clone(),
        }
    }
}

/// Trait allowing the type-omission of [`Actor`][Actor] types but still allowing
/// statically-typed message transmission
#[async_trait]
pub trait MessageReceiver<M: Message>: 'static + Send + Sync {
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

/// Allows type-omission of [`Actor`][Actor] types but still allowing
/// statically-typed message transmission
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

            #[cfg(feature = "remote")]
            Ref::Remote(remote_ref) => match msg.as_bytes() {
                Ok(envelope) => remote_ref.send(Envelope::Remote(envelope)).await,
                Err(e) => Err(ActorRefErr::Serialisation(e)),
            },
        }
    }

    pub async fn notify<Msg: Message>(&self, msg: Msg) -> Result<(), ActorRefErr>
    where
        A: Handler<Msg>,
    {
        match &self.inner_ref {
            Ref::Local(local_ref) => local_ref.notify(msg),

            #[cfg(feature = "remote")]
            Ref::Remote(remote_ref) => match msg.as_bytes() {
                Ok(envelope) => remote_ref.notify(Envelope::Remote(envelope)).await,
                Err(e) => Err(ActorRefErr::Serialisation(e)),
            },
        }
    }

    pub fn is_local(&self) -> bool {
        matches!(&self.inner_ref, &Ref::Local(_))
    }

    #[cfg(feature = "remote")]
    pub fn is_remote(&self) -> bool {
        matches!(&self.inner_ref, &Ref::Remote(_))
    }

    #[cfg(feature = "remote")]
    pub fn unwrap_remote(self) -> RemoteActorRef<A> {
        match self.inner_ref {
            Ref::Remote(remote_ref) => remote_ref,
            _ => panic!("ActorRef does not contain a remote ref"),
        }
    }

    pub fn unwrap_local(self) -> LocalActorRef<A> {
        match self.inner_ref {
            Ref::Local(local_ref) => local_ref,
            _ => panic!("ActorRef does not contain a local ref"),
        }
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

impl<A: Actor> From<LocalActorRef<A>> for ActorRef<A> {
    fn from(r: LocalActorRef<A>) -> Self {
        ActorRef {
            inner_ref: Ref::Local(r),
        }
    }
}

#[cfg(feature = "remote")]
impl<A: Actor> From<RemoteActorRef<A>> for ActorRef<A> {
    fn from(r: RemoteActorRef<A>) -> Self {
        ActorRef {
            inner_ref: Ref::Remote(r),
        }
    }
}

/// Trait defining the core functionality of an [`ActorRef`][ActorRef].
#[async_trait]
pub trait CoreActorRef: Any {
    fn actor_id(&self) -> &ActorId;

    fn actor_path(&self) -> &ActorPath;

    fn actor_type(&self) -> &'static str;

    async fn status(&self) -> Result<ActorStatus, ActorRefErr>;

    async fn stop(&self) -> Result<(), ActorRefErr>;

    fn describe(&self, describe: Describe) -> Result<(), ActorRefErr>;

    fn notify_stop(&self) -> Result<(), ActorRefErr>;

    fn notify_child_terminated(&self, id: ActorId) -> Result<(), ActorRefErr>;

    fn is_valid(&self) -> bool;

    fn as_any(&self) -> &dyn Any;
}

/// A type-omitted reference to an [`Actor`][Actor]
#[derive(Clone)]
pub struct BoxedActorRef(Arc<dyn CoreActorRef + Send + Sync>);

impl BoxedActorRef {
    pub fn as_actor<A: Actor>(&self) -> Option<LocalActorRef<A>> {
        self.0.as_any().downcast_ref::<LocalActorRef<A>>().cloned()
    }
}

impl Display for BoxedActorRef {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "actor_id={}, actor_type={}",
            &self.actor_id(),
            &self.actor_type()
        )
    }
}

/// A reference to a local [`Actor`][Actor] instance
pub struct LocalActorRef<A: Actor> {
    inner: Arc<LocalActorRefInner<A>>,
}

/// Internal shared data of a local [`Actor`][Actor] reference
pub struct LocalActorRefInner<A: Actor> {
    pub id: ActorId,
    path: ActorPath,
    sender: UnboundedSender<MessageHandler<A>>,
}

impl<A: Actor> Hash for LocalActorRef<A> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
        self.path.hash(state);
    }
}

impl<A: Actor> PartialEq<Self> for LocalActorRef<A> {
    fn eq(&self, other: &Self) -> bool {
        self.path == other.path && self.id == other.id
    }
}

impl<A: Actor> Eq for LocalActorRef<A> {}

impl<A: Actor> Debug for LocalActorRef<A> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(&format!("LocalActorRef<{}>", A::type_name()))
            .field("path", &self.inner.path)
            .field("actor_id", &self.inner.id)
            .finish()
    }
}

impl Debug for BoxedActorRef {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BoxedActorRef")
            .field("actor_id", &self.0.actor_id())
            .field("actor_type", &self.0.actor_type())
            .finish()
    }
}

impl<A: Actor> From<LocalActorRef<A>> for BoxedActorRef {
    fn from(r: LocalActorRef<A>) -> Self {
        BoxedActorRef(Arc::new(r))
    }
}

impl<A: Actor> Clone for LocalActorRef<A> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

/// The error type used for [`Coerce`][crate]'s actor-related APIs
#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
pub enum ActorRefErr {
    ActorUnavailable,
    NotFound(ActorId),
    AlreadyExists(ActorId),
    Serialisation(MessageWrapErr),
    Deserialisation(MessageUnwrapErr),
    Timeout {
        time_taken_millis: u64,
    },
    ActorStartFailed,
    InvalidRef,
    ResultChannelClosed,
    ResultSendFailed,
    NotSupported {
        actor_id: ActorId,
        message_type: String,
        actor_type: String,
    },
    NotImplemented,
}

impl Display for ActorRefErr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ActorRefErr::ActorUnavailable => write!(f, "actor unavailable"),
            ActorRefErr::NotFound(id) => write!(f, "actor {} could not be found", &id),
            ActorRefErr::AlreadyExists(id) => write!(f, "actor {} already exists", &id),
            ActorRefErr::Serialisation(err) => write!(f, "serialisation error ({})", &err),
            ActorRefErr::Deserialisation(err) => write!(f, "deserialisation error ({})", &err),
            ActorRefErr::Timeout { time_taken_millis } => {
                write!(f, "timeout (time_taken_millis={})", time_taken_millis)
            }
            ActorRefErr::InvalidRef => write!(f, "failed to send message, ref is no longer valid"),
            ActorRefErr::ResultSendFailed => write!(f, "failed to send result, channel closed"),
            ActorRefErr::ResultChannelClosed => write!(f, "failed to read result, channel closed"),
            ActorRefErr::NotSupported {
                actor_id,
                message_type,
                actor_type,
            } => write!(
                f,
                "Remoting not supported, no Handler<{}> implementation for actor (id={}, type={}) or the actor/message combination is not registered with the RemoteActorSystem configuration",
                message_type, actor_id, actor_type
            ),
            ActorRefErr::ActorStartFailed => write!(f, "actor failed to start, channel closed"),
            ActorRefErr::NotImplemented => write!(f, "functionality is not yet implemented"),
        }
    }
}

impl std::error::Error for ActorRefErr {}

impl<A: Actor> LocalActorRef<A> {
    /// Creates a LocalActorRef instance from an [`ActorId`][ActorId], `UnboundSender<MessageHandler<A>>`,
    /// a system_id (`Uuid`) and an [`ActorPath`][ActorPath].
    ///
    /// Generally this should not be used directly.
    pub fn new(
        id: ActorId,
        sender: UnboundedSender<MessageHandler<A>>,
        system_id: Option<Uuid>,
        path: ActorPath,
    ) -> Self {
        Self {
            inner: Arc::new(LocalActorRefInner { id, path, sender }),
        }
    }

    /// Returns a reference to the [`ActorId`][ActorId] of the target [`Actor`][Actor]
    ///
    /// [`Actor`]: coerce::Actor
    pub fn actor_id(&self) -> &ActorId {
        &self.inner.id
    }

    /// Returns a reference to the [`ActorPath`][ActorPath] of the target [`Actor`][Actor]
    pub fn actor_path(&self) -> &ActorPath {
        &self.inner.path
    }

    /// Sends a message to the target [`Actor`][Actor] and waits for the message to be processed and for
    /// a result to be available.
    ///
    /// # Example
    ///
    /// Send a [`Status`][lifecycle::Status] message (a built-in message that every actor can handle), and return the result,
    /// which is an [`ActorStatus`].
    /// ```rust
    /// use coerce::actor::{
    ///     Actor,
    ///     LocalActorRef,
    ///     context::ActorStatus,
    ///     lifecycle::Status
    /// };
    ///
    /// async fn get_status<A: Actor>(actor: &LocalActorRef<A>) -> Option<ActorStatus> {
    ///     actor.send(Status).await.ok()
    /// }
    ///
    /// ```
    ///
    /// [ActorStatus]: context::ActorStatus
    #[instrument(skip(msg))]
    pub async fn send<Msg: Message>(&self, msg: Msg) -> Result<Msg::Result, ActorRefErr>
    where
        A: Handler<Msg>,
    {
        let message_type = msg.name();
        let actor_type = A::type_name();

        ActorMetrics::incr_messages_sent(A::type_name(), msg.name());

        // let timeout_task = tokio::spawn(async move {
        //    tokio::time::sleep(Duration::from_millis(1000)).await;
        //     info!("message(type={}, actor_type={}) has taken longer than 1000ms", message_type, actor_type);
        // });

        let (tx, rx) = oneshot::channel();
        match self
            .inner
            .sender
            .send(Box::new(ActorMessage::new(msg, Some(tx))))
        {
            Ok(_) => match rx.await {
                Ok(res) => {
                    trace!(
                        "recv result (msg_type={msg_type} actor_type={actor_type})",
                        msg_type = message_type,
                        actor_type = actor_type
                    );

                    Ok(res)
                }
                Err(_e) => Err(ActorRefErr::ResultChannelClosed),
            },
            Err(_e) => Err(ActorRefErr::InvalidRef),
        }
    }

    /// Sends a message to the target [`Actor`][Actor] but doesn't wait for the message to be processed.
    ///
    /// # Example
    ///
    /// Send a [`Stop`][lifecycle::Stop] message, which is a message every actor can handle, but don't wait for it
    /// to be processed.
    ///
    /// ```rust
    /// use coerce::actor::{
    ///     Actor,
    ///     LocalActorRef,
    ///     lifecycle::Stop,
    /// };
    ///
    /// fn stop_actor<A: Actor>(actor: &LocalActorRef<A>) {
    ///     let _ = actor.notify(Stop(None));
    /// }
    ///
    /// ```
    ///
    #[instrument(skip(msg))]
    pub fn notify<Msg: Message>(&self, msg: Msg) -> Result<(), ActorRefErr>
    where
        A: Handler<Msg>,
    {
        ActorMetrics::incr_messages_sent(A::type_name(), msg.name());

        match self
            .inner
            .sender
            .send(Box::new(ActorMessage::new(msg, None)))
        {
            Ok(_) => Ok(()),
            Err(_e) => Err(ActorRefErr::InvalidRef),
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

    /// Sends a [`Status`][lifecycle::Status] message, returning the current [`ActorStatus`][context::ActorStatus]
    pub async fn status(&self) -> Result<ActorStatus, ActorRefErr> {
        self.send(Status).await
    }

    /// Attempts to stop the target `Actor`, waiting for completion
    pub async fn stop(&self) -> Result<(), ActorRefErr> {
        let (tx, rx) = oneshot::channel();
        let res = self.notify(Stop(Some(tx)));
        if res.is_ok() {
            rx.await.map_err(|_| ActorRefErr::InvalidRef)
        } else {
            res
        }
    }

    pub fn describe(&self, describe: Describe) -> Result<(), ActorRefErr> {
        self.notify(describe)
    }

    pub fn is_valid(&self) -> bool {
        !self.inner.sender.is_closed()
    }

    /// Attempts to stop the target `Actor`, without waiting for completion
    pub fn notify_stop(&self) -> Result<(), ActorRefErr> {
        self.notify(Stop(None))
    }
}

#[async_trait]
impl<A: Actor> CoreActorRef for LocalActorRef<A> {
    fn actor_id(&self) -> &ActorId {
        self.actor_id()
    }

    fn actor_path(&self) -> &ActorPath {
        self.actor_path()
    }

    fn actor_type(&self) -> &'static str {
        A::type_name()
    }

    async fn status(&self) -> Result<ActorStatus, ActorRefErr> {
        self.status().await
    }

    async fn stop(&self) -> Result<(), ActorRefErr> {
        self.stop().await
    }

    fn describe(&self, describe: Describe) -> Result<(), ActorRefErr> {
        self.describe(describe)
    }

    fn notify_stop(&self) -> Result<(), ActorRefErr> {
        self.notify_stop()
    }

    fn notify_child_terminated(&self, id: ActorId) -> Result<(), ActorRefErr> {
        self.notify(Terminated(id))
    }

    fn is_valid(&self) -> bool {
        self.is_valid()
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

    fn actor_path(&self) -> &ActorPath {
        self.0.actor_path()
    }

    fn actor_type(&self) -> &'static str {
        self.0.actor_type()
    }

    async fn status(&self) -> Result<ActorStatus, ActorRefErr> {
        self.0.status().await
    }

    async fn stop(&self) -> Result<(), ActorRefErr> {
        self.0.stop().await
    }

    fn describe(&self, describe: Describe) -> Result<(), ActorRefErr> {
        self.0.describe(describe)
    }

    fn notify_stop(&self) -> Result<(), ActorRefErr> {
        self.0.notify_stop()
    }

    fn notify_child_terminated(&self, id: ActorId) -> Result<(), ActorRefErr> {
        self.0.notify_child_terminated(id)
    }

    fn is_valid(&self) -> bool {
        self.0.is_valid()
    }

    fn as_any(&self) -> &dyn Any {
        self
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
