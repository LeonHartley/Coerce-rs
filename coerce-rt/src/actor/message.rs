use crate::actor::Actor;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

pub trait Message {
    type Result;
}

pub type HandleFuture<T> = Pin<Box<dyn Future<Output=T> + Send + Sync>>;

pub trait Handler<Msg: Message + Send + Sync> {
    fn handle(&mut self, message: Msg) -> HandleFuture<Msg::Result>;
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
        M::Result: Send + Sync,
{
    msg: Option<M>,
    sender: Option<tokio::sync::oneshot::Sender<M::Result>>,
    _a: PhantomData<A>,
}

pub trait ActorMessageHandler<A>: Sync + Send
    where
        A: Actor + Sync + Send,
{
    fn handle(&mut self, actor: Arc<tokio::sync::Mutex<A>>) -> HandleFuture<()>;
}

impl<A: 'static + Actor, M: 'static + Message> ActorMessageHandler<A> for ActorMessage<A, M>
    where
        A: Handler<M> + Send + Sync,
        M: Send + Sync,
        M::Result: Send + Sync,
{
    fn handle(&mut self, actor: Arc<tokio::sync::Mutex<A>>) -> HandleFuture<()> {
        self.handle_msg(actor)
    }
}

impl<A: 'static + Actor, M: 'static + Message> ActorMessage<A, M>
    where
        A: Handler<M> + Send + Sync,
        M: Send + Sync,
        M::Result: Send + Sync,
{
    pub fn new(msg: M, sender: tokio::sync::oneshot::Sender<M::Result>) -> ActorMessage<A, M> {
        ActorMessage {
            msg: Some(msg),
            sender: Some(sender),
            _a: PhantomData,
        }
    }

    pub fn handle_msg(&mut self, actor: Arc<tokio::sync::Mutex<A>>) -> HandleFuture<()> {
        async fn run<A: Actor, M: Message>(
            response: tokio::sync::oneshot::Sender<M::Result>,
            message: M,
            actor: Arc<tokio::sync::Mutex<A>>,
        ) where
            A: Handler<M> + Send + Sync,
            M: Send + Sync,
        {
            response.send(actor.as_ref().lock().await.handle(message).await);
        }

        let sender = self.sender.take();
        let msg = self.msg.take();

        Box::pin(run(sender.unwrap(), msg.unwrap(), actor))
    }
}
