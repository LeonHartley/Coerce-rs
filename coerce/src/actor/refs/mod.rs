use crate::actor::context::ActorStatus;
use crate::actor::describe::Describe;
use crate::actor::lifecycle::{Status, Stop};
use crate::actor::message::{
    ActorMessage, Envelope, Exec, Handler, Message, MessageHandler, MessageUnwrapErr,
    MessageWrapErr,
};
use crate::actor::metrics::ActorMetrics;
use crate::actor::supervised::Terminated;
use crate::actor::{Actor, ActorId, ActorPath};
use std::any::Any;
use std::fmt::{Debug, Display, Formatter};
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::sync::Arc;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;

#[cfg(feature = "remote")]
use crate::remote::{actor_ref::RemoteActorRef, system::NodeId};

/// Location-transparent reference to an [`Actor`][Actor].
///
/// Supported targets:
/// - [`LocalActorRef<A>`][LocalActorRef]
/// - [`RemoteActorRef<A>`][crate::remote::RemoteActorRef]
///
pub struct ActorRef<A: Actor> {
    inner_ref: Ref<A>,
}

pub(crate) enum Ref<A: Actor> {
    Local(LocalActorRef<A>),

    #[cfg(feature = "remote")]
    Remote(RemoteActorRef<A>),
}

/// A reference to a local [`Actor`][Actor] instance
pub struct LocalActorRef<A: Actor> {
    inner: Arc<LocalActorRefInner<A>>,
}

impl<A: Actor> LocalActorRef<A> {
    /// Get the direct reference to the actor mailbox's [
    ///
    /// [`UnboundedSender`][tokio::sync::mpsc::unbounded::UnboundedSender]
    pub fn sender(&self) -> &UnboundedSender<MessageHandler<A>> {
        &self.inner.sender
    }
}

/// Internal shared data of a local [`Actor`][Actor] reference
pub struct LocalActorRefInner<A: Actor> {
    pub id: ActorId,
    path: ActorPath,
    sender: UnboundedSender<MessageHandler<A>>,
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
        (self as (&dyn Debug)).fmt(f)
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

impl<A: Actor> Deref for LocalActorRef<A> {
    type Target = LocalActorRefInner<A>;

    #[inline]
    fn deref(&self) -> &LocalActorRefInner<A> {
        self.inner.as_ref()
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
pub struct BoxedActorRef(pub Arc<dyn CoreActorRef + Send + Sync>);

impl BoxedActorRef {
    pub fn actor_id(&self) -> &ActorId {
        self.0.actor_id()
    }

    pub fn actor_type(&self) -> &'static str {
        self.0.actor_type()
    }

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
#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, Clone)]
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
    pub fn new(id: ActorId, sender: UnboundedSender<MessageHandler<A>>, path: ActorPath) -> Self {
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
    #[instrument(skip(msg), level = "debug")]
    pub async fn send<Msg: Message>(&self, msg: Msg) -> Result<Msg::Result, ActorRefErr>
    where
        A: Handler<Msg>,
    {
        let message_type = msg.name();
        let actor_type = A::type_name();

        ActorMetrics::incr_messages_sent(actor_type, message_type);

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

    /// Sends a message to the target [`Actor`][Actor], with the added benefit of passing in a custom oneshot sender,
    /// allowing the use of a separate channel rather than creating one directly as part of the `send` operation
    pub fn deliver<M: Message>(
        &self,
        msg: M,
        result_sender: oneshot::Sender<M::Result>,
    ) -> Result<(), ActorRefErr>
    where
        A: Handler<M>,
    {
        ActorMetrics::incr_messages_sent(A::type_name(), msg.name());

        match self
            .inner
            .sender
            .send(Box::new(ActorMessage::new(msg, Some(result_sender))))
        {
            Ok(_) => Ok(()),
            Err(_) => Err(ActorRefErr::InvalidRef),
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
    #[instrument(skip(msg), level = "debug")]
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
        F: (FnMut(&mut A)) + 'static + Send + Sync,
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

pub trait PipeTo<A: Actor>
where
    A: Handler<Self::Message>,
{
    type Message: Message;

    fn pipe_to(self, actor_ref: ActorRef<A>);
}

impl<A, F: Future> PipeTo<A> for F
where
    F::Output: Message,
    A: Handler<F::Output>,
    F: 'static + Send,
{
    type Message = F::Output;

    fn pipe_to(self, actor_ref: ActorRef<A>) {
        let fut = self;
        tokio::spawn(async move {
            let result = fut.await;
            let _ = actor_ref.notify(result).await;
        });
    }
}
