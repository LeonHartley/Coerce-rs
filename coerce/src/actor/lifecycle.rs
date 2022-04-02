use crate::actor::context::ActorStatus::{Started, Starting, Stopped, Stopping};
use crate::actor::context::{ActorContext, ActorStatus};
use crate::actor::message::{Handler, Message, MessageHandler};
use crate::actor::scheduler::{ActorType, DeregisterActor};
use crate::actor::system::ActorSystem;
use crate::actor::{Actor, BoxedActorRef, LocalActorRef};
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
        if let Some(sender) = stop.0 {
            ctx.add_on_stopped_handler(sender);
        }

        ctx.set_status(Stopping);
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

        if let Some(system) = &system {
            system.metrics().increment_actors_started();
        }

        match ctx.get_status() {
            Stopping => return,
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
                let span = tracing::trace_span!(
                    "Actor::handle",
                    actor_id = ctx.id().as_str(),
                    actor_type_name = A::type_name(),
                    message_type = msg.name()
                );

                let _enter = span.enter();

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

                if let Some(system) = &system {
                    system.metrics().increment_msgs_processed();
                }
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

        actor.stopped(&mut ctx).await;

        ctx.set_status(Stopped);

        if let Some(system) = &system {
            system.metrics().increment_actors_stopped();
        }

        if actor_type.is_tracked() {
            if let Some(system) = system.take() {
                if !system.is_terminated() {
                    trace!("de-registering actor {}", &actor_id);

                    system
                        .scheduler()
                        .send(DeregisterActor(actor_id))
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
}
