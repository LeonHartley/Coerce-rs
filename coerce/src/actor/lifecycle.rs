use crate::actor::context::ActorStatus::{Started, Starting, Stopped, Stopping};
use crate::actor::context::{ActorContext, ActorStatus};
use crate::actor::message::{Handler, Message, MessageHandler};
use crate::actor::scheduler::{ActorType, DeregisterActor};
use crate::actor::system::ActorSystem;
use crate::actor::{Actor, LocalActorRef};

use crate::actor::message::encoding::json::RemoteMessage;
use std::collections::HashMap;

pub struct Status();

pub struct Stop();

impl RemoteMessage for Stop {
    type Result = ();
}

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
    receiver: tokio::sync::mpsc::UnboundedReceiver<MessageHandler<A>>,
    on_start: Option<tokio::sync::oneshot::Sender<bool>>,
    system: Option<ActorSystem>,
}

impl<A: Actor> ActorLoop<A>
where
    A: 'static + Sync + Send,
{
    pub fn new(
        actor: A,
        actor_type: ActorType,
        receiver: tokio::sync::mpsc::UnboundedReceiver<MessageHandler<A>>,
        on_start: Option<tokio::sync::oneshot::Sender<bool>>,
        actor_ref: LocalActorRef<A>,
        system: Option<ActorSystem>,
    ) -> Self {
        ActorLoop {
            actor,
            actor_type,
            actor_ref,
            receiver,
            on_start,
            system,
        }
    }

    pub async fn run(&mut self) {
        let actor_id = self.actor_ref.id.clone();
        let mut ctx = ActorContext::new(
            self.system.clone(),
            Starting,
            self.actor_ref.clone().into(),
            HashMap::new(),
        );
        let system_id = self
            .actor_ref
            .system_id
            .map_or("system-creation".to_string(), |s| s.to_string());

        trace!(
            target:"Actor",
            "[{}] starting on system: {}",
            ctx.id(), system_id
        );

        self.actor.started(&mut ctx).await;

        match ctx.get_status() {
            Stopping => return,
            _ => {}
        };

        ctx.set_status(Started);

        trace!(
            target:"Actor",
            "[{}] ready",
            ctx.id(),
        );

        if let Some(on_start) = self.on_start.take() {
            let _ = on_start.send(true);
        }

        while let Some(mut msg) = self.receiver.recv().await {
            {
                let span = tracing::info_span!(
                    "Actor::handle",
                    actor_id = ctx.id().as_str(),
                    actor_type_name = A::type_name(),
                    message_type = msg.name()
                );

                let _enter = span.enter();

                trace!(
                    target:"Actor",
                    "[{}] recv {}",
                    &actor_id, msg.name()
                );

                msg.handle(&mut self.actor, &mut ctx).await;
            }

            match ctx.get_status() {
                Stopping => break,
                _ => {}
            }
        }

        trace!(
            target:"Actor",
            "[{}] stopping",
            &actor_id
        );

        ctx.set_status(Stopping);

        self.actor.stopped(&mut ctx).await;

        ctx.set_status(Stopped);

        if self.actor_type.is_tracked() {
            if let Some(mut system) = self.system.take() {
                system
                    .scheduler_mut()
                    .send(DeregisterActor(actor_id))
                    .await
                    .expect("de-register actor");
            }
        }
    }
}
