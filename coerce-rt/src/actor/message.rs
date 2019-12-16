use crate::actor::context::ActorHandlerContext;
use crate::actor::Actor;

use std::marker::PhantomData;

pub trait Message {
    type Result;
}

pub(crate) type MessageHandler<A> = Box<dyn ActorMessageHandler<A> + Sync + Send>;

#[async_trait]
pub trait Handler<Msg: Message + Send + Sync>
where
    Msg::Result: Send + Sync,
{
    async fn handle(&mut self, message: Msg, ctx: &mut ActorHandlerContext) -> Msg::Result;
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
    async fn handle(&mut self, actor: &mut A, ctx: &mut ActorHandlerContext) -> ();
}

#[async_trait]
impl<A: 'static + Actor, M: 'static + Message> ActorMessageHandler<A> for ActorMessage<A, M>
where
    A: Handler<M> + Send + Sync,
    M: Send + Sync,
    M::Result: Send + Sync,
{
    async fn handle(&mut self, actor: &mut A, ctx: &mut ActorHandlerContext) -> () {
        self.handle_msg(actor, ctx).await;
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

    pub async fn handle_msg(&mut self, actor: &mut A, ctx: &mut ActorHandlerContext) {
        let msg = self.msg.take();
        let result = actor.handle(msg.unwrap(), ctx).await;

        if let &None = &self.sender {
            return;
        }

        let sender = self.sender.take();
        match sender.unwrap().send(result) {
            Ok(_) => trace!(target: "ActorMessage", "sent result successfully"),
            Err(e) => trace!(target: "ActorMessage", "failed to send result"),
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
    async fn handle(&mut self, message: Exec<F, A, R>, ctx: &mut ActorHandlerContext) -> R {
        let mut message = message;
        let mut func = message.func;

        func(self)
    }
}
