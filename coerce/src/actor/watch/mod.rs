//! Actor watching allows one actor to watch another, ensuring that when the subject stops, the
//! the watching actor will be notified immediately.
//!

use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message};
use crate::actor::{Actor, ActorId, BoxedActorRef, LocalActorRef, Receiver};

pub mod watchers;

#[async_trait]
pub trait ActorWatch {
    fn watch<A: Actor>(&self, actor: LocalActorRef<A>, ctx: &ActorContext);

    fn unwatch<A: Actor>(&self, actor: LocalActorRef<A>, ctx: &ActorContext);
}

impl<A: Actor> ActorWatch for A
where
    A: Handler<ActorTerminated>,
{
    fn watch<W: Actor>(&self, actor: LocalActorRef<W>, ctx: &ActorContext) {
        let terminated_receiver = Receiver::<ActorTerminated>::from(self.actor_ref(ctx));
        let _ = actor.notify(Watch::from(terminated_receiver));
    }

    fn unwatch<W: Actor>(&self, actor: LocalActorRef<W>, ctx: &ActorContext) {
        let _ = actor.notify(Unwatch::from(ctx.id().clone()));
    }
}

#[async_trait]
impl<A: Actor> Handler<Watch> for A {
    async fn handle(&mut self, message: Watch, ctx: &mut ActorContext) {
        ctx.watchers_mut().add_watcher(message.receiver);
    }
}

#[async_trait]
impl<A: Actor> Handler<Unwatch> for A {
    async fn handle(&mut self, message: Unwatch, ctx: &mut ActorContext) {
        ctx.watchers_mut().remove_watcher(message.watcher_id);
    }
}

pub struct Watch {
    receiver: Receiver<ActorTerminated>,
}

impl From<Receiver<ActorTerminated>> for Watch {
    fn from(value: Receiver<ActorTerminated>) -> Self {
        Self { receiver: value }
    }
}

pub struct Unwatch {
    watcher_id: ActorId,
}

impl From<ActorId> for Unwatch {
    fn from(value: ActorId) -> Self {
        Self { watcher_id: value }
    }
}

impl Message for Watch {
    type Result = ();
}

impl Message for Unwatch {
    type Result = ();
}

#[derive(Clone)]
pub struct ActorTerminated {
    actor_ref: BoxedActorRef,
}

impl From<BoxedActorRef> for ActorTerminated {
    fn from(value: BoxedActorRef) -> Self {
        Self { actor_ref: value }
    }
}

impl Message for ActorTerminated {
    type Result = ();
}
