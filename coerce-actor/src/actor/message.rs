use crate::actor::Actor;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;

pub trait Message {
    type Result;
}

pub type HandleFuture<T> = Pin<Box<dyn Future<Output = T> + Send + Sync>>;

pub trait Handler<Msg: Message + Send + Sync> {
    fn handle(&self, message: &Msg) -> HandleFuture<Msg::Result>;
}

pub enum MessageResult<T> {
    Ok(T),
    Error,
}

pub struct ActorMessage<A: Actor, M: Message>
where
    A: Handler<M> + Send + Sync,
    M: Send + Sync,
{
    msg: Arc<M>,
    _a: PhantomData<A>,
}

pub trait ActorMessageHandler<A>: Sync + Send
where
    A: Actor + Sync + Send,
{
    fn handle(&self, actor: Arc<A>) -> HandleFuture<()>;
}

impl<A: 'static + Actor, M: 'static + Message> ActorMessageHandler<A> for ActorMessage<A, M>
where
    A: Handler<M> + Send + Sync,
    M: Send + Sync,
{
    fn handle(&self, actor: Arc<A>) -> HandleFuture<()> {
        self.handle_msg(actor)
    }
}

impl<A: 'static + Actor, M: 'static + Message> ActorMessage<A, M>
where
    A: Handler<M> + Send + Sync,
    M: Send + Sync,
{
    pub fn new(msg: M) -> ActorMessage<A, M> {
        ActorMessage {
            msg: Arc::new(msg),
            _a: PhantomData,
        }
    }

    pub fn handle_msg(&self, actor: Arc<A>) -> HandleFuture<()> {
        async fn run<A: Actor, M: Message>(message: Arc<M>, actor: Arc<A>)
        where
            A: Handler<M> + Send + Sync,
            M: Send + Sync,
        {
            actor.as_ref().handle(message.as_ref()).await;
        }

        Box::pin(run(self.msg.clone(), actor))
    }
}
