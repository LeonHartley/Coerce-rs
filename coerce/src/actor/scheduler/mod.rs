use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message};
use crate::actor::{Actor, ActorId, BoxedActorRef, LocalActorRef};

use crate::actor::lifecycle::ActorLoop;
use crate::actor::system::ActorSystem;
use crate::remote::actor::message::SetRemote;
use crate::remote::system::RemoteActorSystem;
use std::collections::HashMap;
use std::marker::PhantomData;

pub mod timer;

pub struct ActorScheduler {
    pub(crate) actors: HashMap<ActorId, BoxedActorRef>,
    remote: Option<RemoteActorSystem>,
}

impl ActorScheduler {
    pub fn new() -> LocalActorRef<ActorScheduler> {
        start_actor(
            ActorScheduler {
                actors: HashMap::new(),
                remote: None,
            },
            "ActorScheduler-0".to_string(),
            ActorType::Anonymous,
            None,
            None,
            None,
        )
    }
}

#[async_trait]
impl Actor for ActorScheduler {}

#[derive(Debug, Clone, Copy)]
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

pub struct SetSystem(pub ActorSystem);

impl Message for SetSystem {
    type Result = ();
}

pub struct RegisterActor<A: Actor>
where
    A: 'static + Sync + Send,
{
    pub id: ActorId,
    pub actor_ref: LocalActorRef<A>,
}

impl<A: Actor> Message for RegisterActor<A>
where
    A: 'static + Sync + Send,
{
    type Result = ();
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
    type Result = Option<LocalActorRef<A>>;
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
impl Handler<SetRemote> for ActorScheduler {
    async fn handle(&mut self, message: SetRemote, _ctx: &mut ActorContext) {
        self.remote = Some(message.0);
        trace!(target: "ActorScheduler", "actor scheduler is now configured for remoting");
    }
}

#[async_trait]
impl<A: Actor> Handler<RegisterActor<A>> for ActorScheduler
where
    A: 'static + Sync + Send,
{
    async fn handle(&mut self, message: RegisterActor<A>, _ctx: &mut ActorContext) {
        let _ = self
            .actors
            .insert(message.id.clone(), BoxedActorRef::from(message.actor_ref));

        if let Some(remote) = self.remote.as_mut() {
            remote.register_actor(message.id.clone(), None);
        }

        trace!(target: "ActorScheduler", "actor {} registered", message.id);
    }
}

#[async_trait]
impl Handler<DeregisterActor> for ActorScheduler {
    async fn handle(&mut self, msg: DeregisterActor, _ctx: &mut ActorContext) -> () {
        if let Some(_a) = self.actors.remove(&msg.0) {
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
        _ctx: &mut ActorContext,
    ) -> Option<LocalActorRef<A>> {
        self.actors.get(&message.id).map_or(None, |actor| {
            actor
                .1
                .downcast_ref::<LocalActorRef<A>>()
                .map(|s| s.clone())
        })
    }
}

pub fn start_actor<A: Actor>(
    actor: A,
    id: ActorId,
    actor_type: ActorType,
    on_start: Option<tokio::sync::oneshot::Sender<bool>>,
    system: Option<ActorSystem>,
    parent_ref: Option<BoxedActorRef>,
) -> LocalActorRef<A>
where
    A: 'static + Send + Sync,
{
    let actor_id_clone = id.clone();
    let actor_id = actor_id_clone.as_str();
    let actor_type_name = A::type_name();
    tracing::trace_span!(
        "ActorScheduler::start_actor",
        actor_id = actor_id,
        actor_type_name = actor_type_name
    );

    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

    let system_id = system.as_ref().map(|s| *s.system_id());

    let actor_ref = LocalActorRef {
        id,
        sender: tx,
        system_id,
    };

    let cloned_ref = actor_ref.clone();
    tokio::spawn(async move {
        ActorLoop::run(
            actor, actor_type, rx, on_start, cloned_ref, parent_ref, system,
        )
        .await;
    });

    let actor_id = actor_id_clone.as_str();
    tracing::trace!(message = "started actor", actor_id, actor_type_name);
    actor_ref
}
