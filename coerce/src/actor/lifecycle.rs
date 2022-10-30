use crate::actor::context::ActorStatus::{Started, Starting, Stopped, Stopping};
use crate::actor::context::{ActorContext, ActorStatus};
use crate::actor::message::{Handler, Message, MessageHandler};
use crate::actor::metrics::ActorMetrics;
use crate::actor::scheduler::{ActorType, DeregisterActor};
use crate::actor::system::ActorSystem;
use crate::actor::{Actor, ActorId, BoxedActorRef, LocalActorRef};
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedReceiver;

use tokio::sync::oneshot::Sender;

pub struct Status();

pub struct Stop(pub Option<Sender<()>>);

impl Message for Status {
    type Result = ActorStatus;
}

impl Message for Stop {
    type Result = ();
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
    async fn handle(&mut self, stop: Stop, ctx: &mut ActorContext) {
        ctx.stop(stop.0);
    }
}

pub struct ActorLoop {}

impl ActorLoop {
    pub async fn run<A: Actor>(
        mut actor: A,
        actor_type: ActorType,
        mut receiver: UnboundedReceiver<MessageHandler<A>>,
        mut on_start: Option<Sender<()>>,
        actor_ref: LocalActorRef<A>,
        parent_ref: Option<BoxedActorRef>,
        mut system: Option<ActorSystem>,
    ) {
        let actor_id = actor_ref.id.clone();
        let mut ctx = A::new_context(system.clone(), Starting, actor_ref.clone().into())
            .with_parent(parent_ref);

        let system_id = actor_ref
            .system_id
            .map_or("system-creation".to_string(), |s| s.to_string());

        trace!(
            target: "Actor",
            "[{}] starting on system: {}",
            ctx.id(), system_id
        );

        actor.started(&mut ctx).await;

        ActorMetrics::incr_actor_created(A::type_name());

        match ctx.get_status() {
            Stopping => {
                return actor_stopped(&mut actor, actor_type, &mut system, &actor_id, &mut ctx)
                    .await
            }
            _ => {}
        };

        ctx.set_status(Started);

        trace!(
            target: "Actor",
            "[{}] ready",
            ctx.id(),
        );

        if let Some(on_start) = on_start.take() {
            let _ = on_start.send(());
        }

        while let Some(mut msg) = receiver.recv().await {
            {
                // let msg_type = msg.name();
                // let actor_type = A::type_name();
                //
                // let span = tracing::trace_span!(
                //     "Actor::handle",
                //     actor_id = ctx.id().as_ref(),
                //     actor_type_name = actor_type,
                //     message_type = msg_type,
                // );
                //
                // let _enter = span.enter();

                trace!(
                    target: "Actor",
                    "[{}] received {}",
                    &actor_id, msg.name()
                );

                msg.handle(&mut actor, &mut ctx).await;

                trace!(
                    target: "Actor",
                    "[{}] processed {}",
                    &actor_id, msg.name()
                );
            }

            match ctx.get_status() {
                Stopping => break,
                _ => {}
            }
        }

        trace!(
            target: "Actor",
            "[{}] stopping",
            &actor_id
        );

        ctx.set_status(Stopping);
        actor_stopped(&mut actor, actor_type, &mut system, &actor_id, &mut ctx).await
    }
}

async fn actor_stopped<A: Actor>(
    actor: &mut A,
    actor_type: ActorType,
    system: &mut Option<ActorSystem>,
    actor_id: &ActorId,
    mut ctx: &mut ActorContext,
) {
    actor.stopped(&mut ctx).await;

    ctx.set_status(Stopped);

    if actor_type.is_tracked() {
        // TODO: we probably want to do this if the actor exited upon stopping too
        if let Some(system) = system.take() {
            if !system.is_terminated() {
                trace!("de-registering actor {}", &actor_id);

                system
                    .scheduler()
                    .send(DeregisterActor(actor_id.clone()))
                    .await
                    .expect("de-register actor");
            }
        }
    }

    if let Some(on_stopped_handlers) = ctx.take_on_stopped_handlers() {
        for sender in on_stopped_handlers {
            let _ = sender.send(());
        }
    }
}
