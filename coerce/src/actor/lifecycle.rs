//! Actor lifecycle and [`ActorLoop`][ActorLoop] implementation

use crate::actor::context::ActorStatus::{Started, Starting, Stopped, Stopping};
use crate::actor::context::{ActorContext, ActorStatus};
use crate::actor::message::{Handler, Message, MessageHandler};
use crate::actor::metrics::ActorMetrics;
use crate::actor::scheduler::{ActorType, DeregisterActor};
use crate::actor::system::ActorSystem;
use crate::actor::{Actor, ActorId, BoxedActorRef, CoreActorRef, LocalActorRef};

use tokio::sync::mpsc::UnboundedReceiver;
use tracing::Instrument;
use valuable::Valuable;

use crate::actor::watch::ActorTerminated;
use tokio::sync::oneshot::Sender;

pub struct Status;

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
        let actor_id = actor_ref.actor_id().clone();
        let mut ctx = actor
            .new_context(system.clone(), Starting, actor_ref.clone().into())
            .with_parent(parent_ref);

        trace!(actor = ctx.full_path().as_ref(), "actor starting");

        actor.started(&mut ctx).await;
        ActorMetrics::incr_actor_created(A::type_name());

        if ctx.get_status() == &Stopping {
            return actor_stopped(&mut actor, actor_type, &mut system, &actor_id, &mut ctx).await;
        }

        ctx.set_status(Started);

        trace!(actor = ctx.full_path().as_ref(), "actor ready");

        if let Some(on_start) = on_start.take() {
            let _ = on_start.send(());
        }

        let log = ctx.log();
        while let Some(mut msg) = receiver.recv().await {
            {
                #[cfg(feature = "actor-tracing-info")]
                let span = tracing::info_span!(
                    "actor.recv",
                    ctx = log.as_value(),
                    message_type = msg.name(),
                );

                #[cfg(feature = "actor-tracing-debug")]
                let span = tracing::debug_span!(
                    "actor.recv",
                    ctx = log.as_value(),
                    message_type = msg.name(),
                );

                #[cfg(feature = "actor-tracing-trace")]
                let span = tracing::trace_span!(
                    "actor.recv",
                    ctx = log.as_value(),
                    message_type = msg.name(),
                );

                trace!(
                    actor = ctx.full_path().as_ref(),
                    msg_type = msg.name(),
                    "actor message received"
                );

                let handle_fut = msg.handle(&mut actor, &mut ctx);

                #[cfg(feature = "actor-tracing")]
                let handle_fut = handle_fut.instrument(span);

                handle_fut.await;

                trace!(
                    actor = ctx.full_path().as_ref(),
                    msg_type = msg.name(),
                    "actor message processed"
                );
            }

            if ctx.get_status() == &Stopping {
                break;
            }
        }

        trace!(actor = ctx.full_path().as_ref(), "actor stopping");

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
        if let Some(system) = system.take() {
            if !system.is_terminated() {
                trace!(actor_id = actor_id.as_ref(), "de-registering actor");

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

    if let Some(watchers) = ctx.take_watchers() {
        let actor_terminated = ActorTerminated::from(ctx.boxed_actor_ref());
        for watcher in watchers.iter() {
            let _ = watcher.notify(actor_terminated.clone());
        }
    }
}
