use crate::actor::context::ActorStatus::{Started, Starting, Stopped, Stopping};
use crate::actor::context::{ActorContext, ActorStatus, ActorSystem};
use crate::actor::message::{Handler, Message, MessageHandler};
use crate::actor::scheduler::{ActorScheduler, ActorType, DeregisterActor};
use crate::actor::{Actor, ActorId, LocalActorRef};
use std::collections::HashMap;

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
    async fn handle(&mut self, _message: Status, ctx: &mut ActorContext) -> ActorStatus {
        ctx.get_status().clone()
    }
}

#[async_trait]
impl<A: Actor> Handler<Stop> for A
where
    A: 'static + Sync + Send,
{
    async fn handle(&mut self, _message: Stop, ctx: &mut ActorContext) -> ActorStatus {
        ctx.set_status(Stopping);

        Stopping
    }
}

pub async fn actor_loop<A: Actor>(
    id: ActorId,
    mut actor: A,
    actor_type: ActorType,
    mut rx: tokio::sync::mpsc::Receiver<MessageHandler<A>>,
    on_start: Option<tokio::sync::oneshot::Sender<bool>>,
    actor_ref: LocalActorRef<A>,
    system: Option<ActorSystem>,
    scheduler: Option<LocalActorRef<ActorScheduler>>,
) where
    A: 'static + Send + Sync,
{
    let mut ctx = ActorContext::new(
        id.clone(),
        system,
        Starting,
        actor_ref.into(),
        HashMap::new(),
    );
    trace!(target: "ActorLoop", "[{}] starting", ctx.id());

    actor.started(&mut ctx).await;

    match ctx.get_status() {
        Stopping => return,
        _ => {}
    };

    ctx.set_status(Started);

    trace!(target: "ActorLoop", "[{}] ready", ctx.id());

    if let Some(on_start) = on_start {
        let _ = on_start.send(true);
    }

    while let Some(mut msg) = rx.recv().await {
        trace!(target: "ActorLoop", "[{}] recv", ctx.id());

        msg.handle(&mut actor, &mut ctx).await;

        match ctx.get_status() {
            Stopping => break,
            _ => {}
        }
    }

    trace!(target: "ActorLoop", "[{}] stopping", ctx.id());

    ctx.set_status(Stopping);

    actor.stopped(&mut ctx).await;

    ctx.set_status(Stopped);

    if actor_type.is_tracked() {
        if let Some(mut scheduler) = scheduler {
            scheduler
                .send(DeregisterActor(id))
                .await
                .expect("de-register actor");
        }
    }
}
