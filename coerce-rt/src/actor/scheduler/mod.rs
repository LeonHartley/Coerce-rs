use crate::actor::context::ActorHandlerContext;
use crate::actor::lifecycle::actor_loop;
use crate::actor::message::{Handler, Message};
use crate::actor::{Actor, ActorId, ActorRef, BoxedActorRef, GetActorRef};

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
            ActorType::Anonymous,
            None,
            None,
        )
    }
}

#[async_trait]
impl Actor for ActorScheduler {}

#[derive(Clone, Copy)]
pub enum ActorType {
    Tracked,
    Anonymous,
}

impl ActorType {
    pub fn is_tracked(&self) -> bool {
        match &self {
            &ActorType::Tracked => true,
            _ => false,
        }
    }

    pub fn is_anon(&self) -> bool {
        match &self {
            &ActorType::Anonymous => true,
            _ => false,
        }
    }
}

pub struct RegisterActor<A: Actor>(pub A, pub ActorType, pub tokio::sync::oneshot::Sender<bool>)
where
    A: 'static + Sync + Send;

impl<A: Actor> Message for RegisterActor<A>
where
    A: 'static + Sync + Send,
{
    type Result = ActorRef<A>;
}

pub struct DeregisterActor(pub ActorId);

impl Message for DeregisterActor {
    type Result = ();
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
        ctx: &mut ActorHandlerContext,
    ) -> ActorRef<A> {
        let actor_tyoe = message.1;
        let actor = start_actor(
            message.0,
            actor_tyoe,
            Some(message.2),
            Some(self.get_ref(ctx)),
        );

        if actor_tyoe.is_tracked() {
            let _ = self
                .actors
                .insert(actor.id, BoxedActorRef::from(actor.clone()));

            warn!(target: "ActorScheduler", "actor {} registered", actor.id);
        }

        actor
    }
}

#[async_trait]
impl Handler<DeregisterActor> for ActorScheduler {
    async fn handle(&mut self, msg: DeregisterActor, _ctx: &mut ActorHandlerContext) -> () {
        if let Some(a) = self.actors.remove(&msg.0) {
            trace!(target: "ActorScheduler", "de-registered actor {}", msg.0);
        } else {
            warn!(target: "ActorScheduler", "actor {} not found to de-register", msg.0);
        }
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
    actor_type: ActorType,
    on_start: Option<tokio::sync::oneshot::Sender<bool>>,
    scheduler: Option<ActorRef<ActorScheduler>>,
) -> ActorRef<A>
where
    A: 'static + Send + Sync,
{
    let id = ActorId::new_v4();
    let (tx, rx) = tokio::sync::mpsc::channel(128);

    let actor_ref = ActorRef {
        id: id.clone(),
        sender: tx,
    };

    tokio::spawn(actor_loop(
        id.clone(),
        actor,
        actor_type,
        rx,
        on_start,
        actor_ref.clone(),
        scheduler,
    ));

    actor_ref
}
