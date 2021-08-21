use crate::actor::context::ActorStatus::{Started, Starting, Stopped, Stopping};
use crate::actor::context::{ActorContext, ActorStatus};
use crate::actor::message::{Handler, Message, MessageHandler};
use crate::actor::scheduler::{ActorType, DeregisterActor};
use crate::actor::system::ActorSystem;
use crate::actor::{Actor, BoxedActorRef, LocalActorRef};
use slog::Logger;

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
    A: Actor,
{
    async fn handle(&mut self, _message: Status, ctx: &mut ActorContext) -> ActorStatus {
        ctx.get_status().clone()
    }
}

#[async_trait]
impl<A: Actor> Handler<Stop> for A {
    async fn handle(&mut self, _message: Stop, ctx: &mut ActorContext) -> ActorStatus {
        ctx.set_status(Stopping);

        Stopping
    }
}

pub struct ActorLoop {}

impl ActorLoop {
    pub async fn run<A: Actor>(
        mut actor: A,
        actor_type: ActorType,
        mut receiver: tokio::sync::mpsc::UnboundedReceiver<MessageHandler<A>>,
        mut on_start: Option<tokio::sync::oneshot::Sender<()>>,
        actor_ref: LocalActorRef<A>,
        parent_ref: Option<BoxedActorRef>,
        mut system: Option<ActorSystem>,
        fallback_logger: Option<Logger>
    ) {
        let actor_id = actor_ref.id.clone();
        let mut ctx = A::new_context(system.clone(), Starting, actor_ref.clone().into())
            .with_parent(parent_ref)
            .with_logger(system.as_ref().map_or(fallback_logger, |s| {
                Some(s.log().new(o!(
                "context" => "Actor",
                "actor-id" => actor_id.clone(),
                "actor-type" => A::type_name()
                )))
            }));

        let system_id = actor_ref
            .system_id
            .map_or("system-creation".to_string(), |s| s.to_string());

        trace!(
            ctx.log(),
            "[{}] starting on system: {}",
            ctx.id(),
            system_id
        );

        actor.started(&mut ctx).await;

        match ctx.get_status() {
            Stopping => return,
            _ => {}
        };

        ctx.set_status(Started);

        trace!(ctx.log(), "[{}] ready", ctx.id(),);

        if let Some(on_start) = on_start.take() {
            let _ = on_start.send(());
        }

        while let Some(mut msg) = receiver.recv().await {
            {
                let span = tracing::info_span!(
                    "Actor::handle",
                    actor_id = ctx.id().as_str(),
                    actor_type_name = A::type_name(),
                    message_type = msg.name()
                );

                let _enter = span.enter();

                trace!(ctx.log(), "[{}] recv {}", &actor_id, msg.name());

                msg.handle(&mut actor, &mut ctx).await;
            }

            match ctx.get_status() {
                Stopping => break,
                _ => {}
            }
        }

        trace!(ctx.log(), "[{}] stopping", &actor_id);

        ctx.set_status(Stopping);

        actor.stopped(&mut ctx).await;

        ctx.set_status(Stopped);

        if actor_type.is_tracked() {
            if let Some(system) = system.take() {
                if !system.is_terminated() {
                    trace!(ctx.log(), "de-registering actor {}", &actor_id);

                    system
                        .scheduler()
                        .send(DeregisterActor(actor_id))
                        .await
                        .expect("de-register actor");
                }
            }
        }
    }
}
