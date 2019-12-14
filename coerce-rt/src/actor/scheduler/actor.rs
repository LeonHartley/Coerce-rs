use crate::actor::message::{Message, Handler};
use crate::actor::scheduler::ActorScheduler;
use crate::actor::{Actor, ActorRef, ActorId, BoxedActorRef};
use crate::actor::context::ActorHandlerContext;
use crate::actor::lifecycle::actor_loop;

#[async_trait]
impl Actor for ActorScheduler {}

pub struct RegisterActor<A: Actor>(pub A) where A: 'static + Sync + Send;

impl<A: Actor> Message for RegisterActor<A> where A: 'static + Sync + Send {
    type Result = ActorRef<A>;
}

#[async_trait]
impl<A: Actor> Handler<RegisterActor<A>> for ActorScheduler where A: 'static + Sync + Send {
    async fn handle(&mut self, message: RegisterActor<A>, ctx: &mut ActorHandlerContext) -> ActorRef<A>
    {
        let actor = start_actor(message.0);

        let _ = self
            .actors
            .insert(actor.id, BoxedActorRef::from(actor.clone()));

        actor
    }
}

pub fn start_actor<A: Actor>(actor: A) -> ActorRef<A> where A: 'static + Send + Sync {
    let id = ActorId::new_v4();
    let (tx, rx) = tokio::sync::mpsc::channel(128);

    tokio::spawn(actor_loop(actor, rx));

    ActorRef {
        id: id.clone(),
        sender: tx.clone(),
    }
}
