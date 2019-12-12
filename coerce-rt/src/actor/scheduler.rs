use crate::actor::context::ActorContext;
use crate::actor::message::{ActorMessage, ActorMessageHandler, Handler, Message, MessageResult};
use crate::actor::{Actor, ActorId};
use std::any::{Any, TypeId};
use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::mem::transmute;
use std::ops::DerefMut;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

pub struct ActorScheduler {}

impl ActorScheduler {
    pub fn new() -> ActorScheduler {
        ActorScheduler {}
    }

    pub fn register<A: Actor + Sync + Send>(
        &self,
        mut actor: A,
        ctx: Arc<Mutex<ActorContext>>,
    ) -> ActorRef<A> {
        let id = ActorId::new_v4();
        let (mut tx, mut rx) =
            tokio::sync::mpsc::channel::<Box<dyn ActorMessageHandler<A> + Sync + Send>>(100);

        let actor_ref = ActorRef {
            id,
            context: ctx.clone(),
            sender: tx.clone(),
        };

        tokio::spawn(async move {
            actor.started().await;

            while let Some(mut msg) = rx.recv().await {
                msg.handle(&mut actor).await;
            }

            actor.stopped().await;
        });

        actor_ref
    }

    pub fn run<F, A: Actor>(actor: &mut A, callback: &mut F)
    where
        F: (FnMut(&mut A) -> ()),
    {
        callback(actor)
    }
}

pub struct ActorRef<A: Actor + Sync + Send + 'static> {
    id: Uuid,
    context: Arc<Mutex<ActorContext>>,
    sender: tokio::sync::mpsc::Sender<Box<dyn ActorMessageHandler<A> + Sync + Send>>,
}

impl<A> Clone for ActorRef<A>
where
    A: Actor + Sync + Send + 'static,
{
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            context: self.context.clone(),
            sender: self.sender.clone(),
        }
    }
}

impl<A> ActorRef<A>
where
    A: Actor + Sync + Send + 'static,
{
    pub async fn send<Msg: Message + Sync + Send + 'static>(
        &mut self,
        msg: Msg,
    ) -> MessageResult<Msg::Result>
    where
        A: Handler<Msg>,
        Msg::Result: Send + Sync,
    {
        let (mut tx, rx) = tokio::sync::oneshot::channel();
        self.sender.send(Box::new(ActorMessage::new(msg, tx))).await;

        match rx.await {
            Ok(res) => MessageResult::Ok(res),
            Err(e) => {
                println!("{:?}", e);
                MessageResult::Error
            }
        }
    }
}
