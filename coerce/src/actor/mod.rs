use crate::actor::context::{ActorContext, ActorStatus};
use crate::actor::lifecycle::{Status, Stop};
use crate::actor::message::{
    ActorMessage, Envelope, Exec, Handler, Message, MessageHandler, MessageUnwrapErr,
    MessageWrapErr,
};
use crate::actor::metrics::ActorMetrics;
use crate::actor::scheduler::ActorType::{Anonymous, Tracked};
use crate::actor::supervised::Terminated;
use crate::actor::system::ActorSystem;
use crate::remote::system::NodeId;
use crate::remote::RemoteActorRef;
use std::any::Any;
use std::fmt::{Debug, Display, Formatter};
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;

use crate::actor::message::describe::{ActorDescription, Describe};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub mod context;
pub mod lifecycle;
pub mod message;
pub mod metrics;
pub mod scheduler;
pub mod supervised;
pub mod system;
pub mod worker;

pub type ActorId = Arc<str>;

pub trait IntoActorId {
    fn into_actor_id(self) -> ActorId;
}

pub trait ToActorId {
    fn to_actor_id(&self) -> ActorId;
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

    async fn on_child_stopped(&mut self, _id: &ActorId, _ctx: &mut ActorContext) {}

    fn actor_ref(&self, ctx: &ActorContext) -> LocalActorRef<Self>
    where
        Self: Sized,
    {
        ctx.actor_ref()
    }

    fn type_name() -> &'static str
    where
        Self: Sized,
    {
        std::any::type_name::<Self>()
    }
}

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
    fn read_from_bytes(bytes: &Vec<u8>) -> Option<Self>;

    fn write_to_bytes(&self) -> Option<Vec<u8>>;
}

impl ActorRecipe for () {
    fn read_from_bytes(_: &Vec<u8>) -> Option<Self> {
        Some(())
    }

    fn write_to_bytes(&self) -> Option<Vec<u8>> {
        Some(vec![])
    }
}

#[async_trait]
pub trait ActorFactory: Clone {
    type Actor: Actor + 'static + Sync + Send;
    type Recipe: ActorRecipe + 'static + Sync + Send;

    async fn create(&self, recipe: Self::Recipe) -> Result<Self::Actor, ActorCreationErr>;
}

pub async fn new_actor<A: Actor>(actor: A) -> Result<LocalActorRef<A>, ActorRefErr>
where
    A: 'static + Sync + Send,
{
    ActorSystem::global_system().new_tracked_actor(actor).await
}

pub async fn get_actor<A: Actor>(id: ActorId) -> Option<LocalActorRef<A>>
where
    A: 'static + Sync + Send,
{
    ActorSystem::global_system().get_tracked_actor(id).await
}

pub(crate) enum Ref<A: Actor> {
    Local(LocalActorRef<A>),
    Remote(RemoteActorRef<A>),
}

impl<A: Actor> Clone for Ref<A> {
    fn clone(&self) -> Self {
        match &self {
            Ref::Local(a) => Ref::Local(a.clone()),
            Ref::Remote(a) => Ref::Remote(a.clone()),
        }
    }
}

pub struct ActorRef<A: Actor> {
    inner_ref: Ref<A>,
}

impl<A: Actor> Debug for ActorRef<A> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.inner_ref {
            Ref::Local(local_ref) => local_ref.fmt(f),
            Ref::Remote(remote_ref) => remote_ref.fmt(f),
        }
    }
}

impl<A: Actor> Display for ActorRef<A> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.inner_ref {
            Ref::Local(local_ref) => local_ref.fmt(f),
            Ref::Remote(remote_ref) => remote_ref.fmt(f),
        }
    }
}

impl<A: Actor> ActorRef<A> {
    pub fn actor_id(&self) -> &ActorId {
        match &self.inner_ref {
            Ref::Local(a) => &a.id,
            Ref::Remote(a) => a.actor_id(),
        }
    }

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
            Ref::Remote(remote_ref) => match msg.as_bytes() {
                Ok(envelope) => remote_ref.notify(Envelope::Remote(envelope)).await,
                Err(e) => Err(ActorRefErr::Serialisation(e)),
            },
        }
    }

    pub fn is_local(&self) -> bool {
        matches!(&self.inner_ref, &Ref::Local(_))
    }

    pub fn is_remote(&self) -> bool {
        matches!(&self.inner_ref, &Ref::Remote(_))
    }

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

    fn actor_type(&self) -> &'static str;

    async fn status(&self) -> Result<ActorStatus, ActorRefErr>;

    async fn stop(&self) -> Result<(), ActorRefErr>;

    fn describe(&self, describe: Describe) -> Result<(), ActorRefErr>;

    fn notify_stop(&self) -> Result<(), ActorRefErr>;

    fn notify_child_terminated(&self, id: ActorId) -> Result<(), ActorRefErr>;

    fn is_valid(&self) -> bool;

    fn as_any(&self) -> &dyn Any;
}

#[derive(Clone)]
pub struct BoxedActorRef(Arc<dyn CoreActorRef + Send + Sync>);

impl BoxedActorRef {
    pub fn as_actor<A: Actor>(&self) -> Option<LocalActorRef<A>> {
        self.0.as_any().downcast_ref::<LocalActorRef<A>>().cloned()
    }
}

pub struct LocalActorRef<A: Actor> {
    pub id: ActorId,
    pub system_id: Option<Uuid>,
    sender: tokio::sync::mpsc::UnboundedSender<MessageHandler<A>>,
}

impl<A: Actor> Debug for LocalActorRef<A> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(&format!("LocalActorRef<{}>", A::type_name()))
            .field("actor_id", &self.id)
            .field("system_id", &self.system_id)
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
            id: self.id.clone(),
            system_id: self.system_id,
            sender: self.sender.clone(),
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
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
    pub async fn send<Msg: Message>(&self, msg: Msg) -> Result<Msg::Result, ActorRefErr>
    where
        A: Handler<Msg>,
    {
        let message_type = msg.name();
        let actor_type = A::type_name();
        // let span = tracing::trace_span!("LocalActorRef::send", actor_type, message_type);
        // let _enter = span.enter();

        ActorMetrics::incr_messages_sent(A::type_name(), msg.name());

        // let timeout_task = tokio::spawn(async move {
        //    tokio::time::sleep(Duration::from_millis(1000)).await;
        //     info!("message(type={}, actor_type={}) has taken longer than 1000ms", message_type, actor_type);
        // });

        let (tx, rx) = tokio::sync::oneshot::channel();
        match self.sender.send(Box::new(ActorMessage::new(msg, Some(tx)))) {
            Ok(_) => match rx.await {
                Ok(res) => {
                    tracing::trace!(
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

    pub fn notify<Msg: Message>(&self, msg: Msg) -> Result<(), ActorRefErr>
    where
        A: Handler<Msg>,
    {
        // let message_type = msg.name();
        // let span = tracing::span!(
        //     tracing::Level::TRACE,
        //     "LocalActorRef::notify",
        //     message_type = message_type
        // );
        //
        // let _enter = span.enter();

        ActorMetrics::incr_messages_sent(A::type_name(), msg.name());

        match self.sender.send(Box::new(ActorMessage::new(msg, None))) {
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

    pub fn actor_id(&self) -> &ActorId {
        &self.id
    }

    pub async fn status(&self) -> Result<ActorStatus, ActorRefErr> {
        self.send(Status {}).await
    }

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
        !self.sender.is_closed()
    }

    pub fn notify_stop(&self) -> Result<(), ActorRefErr> {
        self.notify(Stop(None))
    }
}

#[async_trait]
impl<A: Actor> CoreActorRef for LocalActorRef<A> {
    fn actor_id(&self) -> &ActorId {
        self.actor_id()
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

pub struct ScheduledNotify<A: Actor, M: Message> {
    cancellation_token: CancellationToken,
    _a: PhantomData<A>,
    _m: PhantomData<M>,
}

impl<A: Actor, M: Message> ScheduledNotify<A, M> {
    pub fn new(cancellation_token: CancellationToken) -> Self {
        ScheduledNotify {
            cancellation_token,
            _a: PhantomData,
            _m: PhantomData,
        }
    }

    pub fn cancel(&self) {
        self.cancellation_token.cancel();
    }
}

impl<A: Actor> LocalActorRef<A> {
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

pub type ActorTag = Box<str>;

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

pub fn new_actor_id() -> ActorId {
    Uuid::new_v4().into_actor_id()
}
