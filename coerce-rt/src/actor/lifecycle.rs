use crate::actor::context::ActorStatus::{Started, Starting, Stopped, Stopping};
use crate::actor::context::{ActorHandlerContext, ActorStatus};
use crate::actor::message::{Handler, Message, MessageHandler};
use crate::actor::Actor;

pub struct Status();

pub struct Stop();

impl Message for Status {
    type Result = ActorStatus;
}

impl Message for Stop {
    type Result = ActorStatus;
}

#[async_trait]
impl<A> Handler<Status> for A
where
    A: 'static + Actor + Sync + Send,
{
    async fn handle(&mut self, _message: Status, ctx: &mut ActorHandlerContext) -> ActorStatus {
        ctx.get_status().clone()
    }
}

#[async_trait]
impl<A: Actor> Handler<Stop> for A
where
    A: 'static + Sync + Send,
{
    async fn handle(&mut self, _message: Stop, ctx: &mut ActorHandlerContext) -> ActorStatus {
        ctx.set_status(Stopping);

        Stopping
    }
}

pub async fn actor_loop<A: Actor>(
    mut actor: A,
    mut rx: tokio::sync::mpsc::Receiver<MessageHandler<A>>,
    on_start: Option<tokio::sync::oneshot::Sender<bool>>,
) where
    A: 'static + Send + Sync,
{
    println!("actor starting");
    let mut ctx = ActorHandlerContext::new(Starting);

    actor.started(&mut ctx).await;

    match ctx.get_status() {
        Stopping => return,
        _ => {}
    };

    ctx.set_status(Started);

    if let Some(on_start) = on_start {
        on_start.send(true);
    }


    println!("actor begin handling msgs");
    while let Some(mut msg) = rx.recv().await {
        println!("actor recv");
        msg.handle(&mut actor, &mut ctx).await;

        match ctx.get_status() {
            Stopping => break,
            _ => {}
        }
    }

    println!("actor stopping");

    ctx.set_status(Stopping);

    actor.stopped(&mut ctx).await;

    ctx.set_status(Stopped);
}
