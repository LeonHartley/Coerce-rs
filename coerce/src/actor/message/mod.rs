use crate::actor::context::ActorContext;
use crate::actor::Actor;

use std::marker::PhantomData;

pub mod encoding;

pub enum Envelope<M: Message> {
    Local(M),
    Remote(Vec<u8>),
}

pub enum EnvelopeType {
    Local,
    Remote,
}

#[derive(Debug, Eq, PartialEq)]
pub enum MessageWrapErr {
    NotTransmittable,
    SerializationErr,
}
#[derive(Debug, Eq, PartialEq)]
pub enum MessageUnwrapErr {
    NotTransmittable,
    DeserializationErr,
}

pub trait Message: 'static + Sync + Send + Sized {
    type Result: 'static + Sync + Send;

    fn into_envelope(self, envelope_type: EnvelopeType) -> Result<Envelope<Self>, MessageWrapErr> {
        match envelope_type {
            EnvelopeType::Local => Ok(Envelope::Local(self)),
            EnvelopeType::Remote => self.into_remote_envelope(),
        }
    }

    fn into_remote_envelope(self) -> Result<Envelope<Self>, MessageWrapErr> {
        Err(MessageWrapErr::NotTransmittable)
    }

    fn from_envelope(envelope: Envelope<Self>) -> Result<Self, MessageUnwrapErr> {
        match envelope {
            Envelope::Local(msg) => Ok(msg),
            Envelope::Remote(bytes) => Self::from_remote_envelope(bytes),
        }
    }

    fn from_remote_envelope(_: Vec<u8>) -> Result<Self, MessageUnwrapErr> {
        Err(MessageUnwrapErr::NotTransmittable)
    }

    fn read_remote_result(_: Vec<u8>) -> Result<Self::Result, MessageUnwrapErr> {
        Err(MessageUnwrapErr::NotTransmittable)
    }

    fn write_remote_result(_res: Self::Result) -> Result<Vec<u8>, MessageWrapErr> {
        Err(MessageWrapErr::NotTransmittable)
    }

    fn name(&self) -> &str {
        std::any::type_name::<Self>()
    }

    fn type_name() -> &'static str {
        std::any::type_name::<Self>()
    }
}

pub(crate) type MessageHandler<A> = Box<dyn ActorMessageHandler<A> + Sync + Send>;

#[async_trait]
pub trait Handler<Msg: Message + Send + Sync>
where
    Msg::Result: Send + Sync,
{
    async fn handle(&mut self, message: Msg, ctx: &mut ActorContext) -> Msg::Result;
}

#[derive(Debug)]
pub enum MessageResult<T> {
    Ok(T),
    Error,
}

pub struct ActorMessage<A: Actor, M: Message>
where
    A: Handler<M> + Send + Sync,
    M: Send + Sync,
    M::Result: 'static + Send + Sync,
{
    msg: Option<M>,
    sender: Option<tokio::sync::oneshot::Sender<M::Result>>,
    _a: PhantomData<A>,
}

#[async_trait]
pub trait ActorMessageHandler<A>: Sync + Send
where
    A: Actor + Sync + Send,
{
    async fn handle(&mut self, actor: &mut A, ctx: &mut ActorContext);

    fn name(&self) -> &str;
}

#[async_trait]
impl<A: 'static + Actor, M: 'static + Message> ActorMessageHandler<A> for ActorMessage<A, M>
where
    A: Handler<M> + Send + Sync,
    M: Send + Sync,
    M::Result: Send + Sync,
{
    async fn handle(&mut self, actor: &mut A, ctx: &mut ActorContext) -> () {
        self.handle_msg(actor, ctx).await;
    }

    fn name(&self) -> &str {
        std::any::type_name::<M>()
    }
}

impl<A: 'static + Actor, M: 'static + Message> ActorMessage<A, M>
where
    A: Handler<M> + Send + Sync,
    M: Send + Sync,
    M::Result: Send + Sync,
{
    pub fn new(
        msg: M,
        sender: Option<tokio::sync::oneshot::Sender<M::Result>>,
    ) -> ActorMessage<A, M> {
        ActorMessage {
            msg: Some(msg),
            sender,
            _a: PhantomData,
        }
    }

    pub async fn handle_msg(&mut self, actor: &mut A, ctx: &mut ActorContext) {
        let msg = self.msg.take();
        let result = actor.handle(msg.unwrap(), ctx).await;

        if let &None = &self.sender {
            trace!(target: "ActorMessage", "no result consumer, message handling complete");
            return;
        }

        let sender = self.sender.take();
        match sender.unwrap().send(result) {
            Ok(_) => trace!(target: "ActorMessage", "sent result successfully"),
            Err(_e) => warn!(target: "ActorMessage", "failed to send result"),
        }
    }
}

pub struct Exec<F, A, R>
where
    F: (FnMut(&mut A) -> R),
{
    func: F,
    _a: PhantomData<A>,
}

impl<F, A, R> Exec<F, A, R>
where
    F: (FnMut(&mut A) -> R),
{
    pub fn new(f: F) -> Exec<F, A, R> {
        Exec {
            func: f,
            _a: PhantomData,
        }
    }
}

impl<F, A, R> Message for Exec<F, A, R>
where
    for<'r> F: (FnMut(&mut A) -> R) + 'static + Send + Sync,
    A: Actor,
    R: 'static + Send + Sync,
{
    type Result = R;
}

#[async_trait]
impl<F, A, R> Handler<Exec<F, A, R>> for A
where
    A: 'static + Actor + Sync + Send,
    F: (FnMut(&mut A) -> R) + 'static + Send + Sync,
    R: 'static + Send + Sync,
{
    async fn handle(&mut self, message: Exec<F, A, R>, _ctx: &mut ActorContext) -> R {
        let message = message;
        let mut func = message.func;

        func(self)
    }
}
