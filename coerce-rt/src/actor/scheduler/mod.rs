use crate::actor::context::ActorHandlerContext;
use crate::actor::lifecycle::actor_loop;
use crate::actor::message::{Handler, Message};
use crate::actor::{Actor, ActorId, ActorRef, BoxedActorRef};

use std::collections::HashMap;
use std::marker::PhantomData;

pub mod timer;

pub struct ActorScheduler {
    actors: HashMap<ActorId, BoxedActorRef>,
}

impl ActorScheduler {
    pub fn new() -> ActorRef<ActorScheduler> {
        start_actor(
            ActorScheduler {
                actors: HashMap::new(),
            },
            None,
        )
    }
}

#[async_trait]
impl Actor for ActorScheduler {}

pub struct RegisterActor<A: Actor>(pub A, pub tokio::sync::oneshot::Sender<bool>)
where
    A: 'static + Sync + Send;

impl<A: Actor> Message for RegisterActor<A>
where
    A: 'static + Sync + Send,
{
    type Result = ActorRef<A>;
}

pub struct GetActor<A: Actor>
where
    A: 'static + Sync + Send,
{
    id: ActorId,
    _a: PhantomData<A>,
}

impl<A: Actor> Message for GetActor<A>
where
    A: 'static + Sync + Send,
{
    type Result = Option<ActorRef<A>>;
}

impl<A: Actor> GetActor<A>
where
    A: 'static + Sync + Send,
{
    pub fn new(id: ActorId) -> GetActor<A> {
        GetActor {
            id,
            _a: PhantomData,
        }
    }
}

#[async_trait]
impl<A: Actor> Handler<RegisterActor<A>> for ActorScheduler
where
    A: 'static + Sync + Send,
{
    async fn handle(
        &mut self,
        message: RegisterActor<A>,
        _ctx: &mut ActorHandlerContext,
    ) -> ActorRef<A> {
        let actor = start_actor(message.0, Some(message.1));

        let _ = self
            .actors
            .insert(actor.id, BoxedActorRef::from(actor.clone()));

        actor
    }
}

#[async_trait]
impl<A: Actor> Handler<GetActor<A>> for ActorScheduler
where
    A: 'static + Sync + Send,
{
    async fn handle(
        &mut self,
        message: GetActor<A>,
        _ctx: &mut ActorHandlerContext,
    ) -> Option<ActorRef<A>> {
        match self.actors.get(&message.id) {
            Some(actor) => Some(ActorRef::<A>::from(actor.clone())),
            None => None,
        }
    }
}

fn start_actor<A: Actor>(
    actor: A,
    on_start: Option<tokio::sync::oneshot::Sender<bool>>,
) -> ActorRef<A>
where
    A: 'static + Send + Sync,
{
    let id = ActorId::new_v4();
    let (tx, rx) = tokio::sync::mpsc::channel(128);

    tokio::spawn(actor_loop(id.clone(), actor, rx, on_start));

    ActorRef {
        id: id.clone(),
        sender: tx,
    }
}
