use crate::actor::context::ActorStatus::{Started, Starting, Stopped, Stopping};
use crate::actor::context::{ActorContext, ActorStatus};
use crate::actor::message::{Handler, Message, MessageHandler};
use crate::actor::scheduler::{ActorScheduler, ActorType, DeregisterActor};
use crate::actor::system::ActorSystem;
use crate::actor::{Actor, ActorId, LocalActorRef};
use std::collections::HashMap;
use std::future::Future;

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

pub struct ActorLoop<A>
where
    A: 'static + Actor + Sync + Send,
{
    actor: A,
    actor_type: ActorType,
    actor_ref: LocalActorRef<A>,
    receiver: tokio::sync::mpsc::Receiver<MessageHandler<A>>,
    on_start: Option<tokio::sync::oneshot::Sender<bool>>,
    scheduler: Option<LocalActorRef<ActorScheduler>>,
}

impl<A: Actor> ActorLoop<A>
where
    A: 'static + Sync + Send,
{
    pub fn new(
        actor: A,
        actor_type: ActorType,
        receiver: tokio::sync::mpsc::Receiver<MessageHandler<A>>,
        on_start: Option<tokio::sync::oneshot::Sender<bool>>,
        actor_ref: LocalActorRef<A>,
        scheduler: Option<LocalActorRef<ActorScheduler>>,
    ) -> Self {
        ActorLoop {
            actor,
            actor_type,
            actor_ref,
            receiver,
            on_start,
            scheduler,
        }
    }

    pub async fn run(&mut self, system: Option<ActorSystem>) {
        let actor_id = self.actor_ref.id.clone();
        let system_id = self
            .actor_ref
            .system_id
            .map_or("NO_SYS".to_string(), |s| s.to_string());

        let mut ctx = ActorContext::new(
            system,
            Starting,
            self.actor_ref.clone().into(),
            HashMap::new(),
        );

        trace!(target: "ActorLoop", "[System: {}], [{}] starting", system_id, ctx.id());

        self.actor.started(&mut ctx).await;

        match ctx.get_status() {
            Stopping => return,
            _ => {}
        };

        ctx.set_status(Started);

        trace!(target: "ActorLoop", "[{}] ready", ctx.id());

        if let Some(on_start) = self.on_start.take() {
            let _ = on_start.send(true);
        }

        while let Some(mut msg) = self.receiver.recv().await {
            trace!(target: "ActorLoop", "[{}] recv", &actor_id);

            msg.handle(&mut self.actor, &mut ctx).await;

            match ctx.get_status() {
                Stopping => break,
                _ => {}
            }
        }

        trace!(target: "ActorLoop", "[{}] stopping", &actor_id);

        ctx.set_status(Stopping);

        self.actor.stopped(&mut ctx).await;

        ctx.set_status(Stopped);

        if self.actor_type.is_tracked() {
            if let Some(mut scheduler) = self.scheduler.take() {
                scheduler
                    .send(DeregisterActor(actor_id))
                    .await
                    .expect("de-register actor");
            }
        }
    }
}
