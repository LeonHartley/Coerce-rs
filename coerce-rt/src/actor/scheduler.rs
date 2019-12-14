use crate::actor::context::ActorStatus::{Started, Starting, Stopped, Stopping};
use crate::actor::context::{ActorContext, ActorHandlerContext, ActorStatus};
use crate::actor::message::{ActorMessage, ActorMessageHandler, Handler, Message, MessageResult};
use crate::actor::{Actor, ActorId};
use std::any::{Any, TypeId};
use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::mem::transmute;
use std::ops::DerefMut;
use std::sync::{Arc, Mutex};
use uuid::Uuid;
use crate::actor::lifecycle::Stop;

pub struct ActorScheduler {}

pub type MessageHandler<A> = Box<dyn ActorMessageHandler<A> + Sync + Send>;

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
        let (mut tx, mut rx) = tokio::sync::mpsc::channel::<MessageHandler<A>>(100);

        let actor_ref = ActorRef {
            id,
            context: ctx.clone(),
            sender: tx.clone(),
        };

        tokio::spawn(async move {
            let mut ctx = ActorHandlerContext::new(Starting);

            actor.started(&mut ctx).await;

            match ctx.get_status() {
                Stopping => {}
                _ => {}
            };

            ctx.set_status(Started);

            while let Some(mut msg) = rx.recv().await {
                msg.handle(&mut actor, &mut ctx).await;

                match ctx.get_status() {
                    Stopping => break,
                    _ => {}
                }
            }

            ctx.set_status(Stopping);

            actor.stopped(&mut ctx).await;

            ctx.set_status(Stopped);
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
    sender: tokio::sync::mpsc::Sender<MessageHandler<A>>,
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

#[derive(Debug, Eq, PartialEq)]
pub enum ActorRefError {
    ActorUnavailable,
}

impl<A> ActorRef<A>
where
    A: Actor + Sync + Send + 'static,
{
    pub async fn send<Msg: Message + Sync + Send + 'static>(
        &mut self,
        msg: Msg,
    ) -> Result<Msg::Result, ActorRefError>
    where
        A: Handler<Msg>,
        Msg::Result: Send + Sync,
    {
        let (mut tx, rx) = tokio::sync::oneshot::channel();
        self.sender.send(Box::new(ActorMessage::new(msg, tx))).await;

        match rx.await {
            Ok(res) => Ok(res),
            Err(e) => {
                println!("{:?}", e);
                Err(ActorRefError::ActorUnavailable)
            }
        }
    }

    pub async fn stop(&mut self) -> Result<ActorStatus, ActorRefError> {
        self.send(Stop{}).await
    }
}
