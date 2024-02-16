//! Actor Scheduling and [`ActorRef`][super::ActorRef] registry
use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message};
use crate::actor::{
    Actor, ActorId, ActorPath, BoxedActorRef, CoreActorRef, IntoActorId, LocalActorRef,
};

use crate::actor::lifecycle::ActorLoop;
use crate::actor::system::ActorSystem;

#[cfg(feature = "remote")]
use crate::remote::{actor::message::SetRemote, system::RemoteActorSystem};

use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;
use uuid::Uuid;

pub mod timer;

pub struct ActorScheduler {
    pub(crate) actors: HashMap<ActorId, BoxedActorRef>,
    system_id: Uuid,

    #[cfg(feature = "remote")]
    remote: Option<RemoteActorSystem>,
}

impl ActorScheduler {
    pub fn new(system_id: Uuid, system_name: Arc<str>) -> LocalActorRef<ActorScheduler> {
        start_actor(
            ActorScheduler {
                system_id,
                actors: HashMap::new(),

                #[cfg(feature = "remote")]
                remote: None,
            },
            "actor-scheduler".into_actor_id(),
            ActorType::Anonymous,
            None,
            None,
            None,
            system_name,
        )
    }
}

#[async_trait]
impl Actor for ActorScheduler {
    async fn started(&mut self, ctx: &mut ActorContext) {
        debug!(actor_id = ctx.id().as_ref(), "scheduler started");
    }

    async fn stopped(&mut self, _ctx: &mut ActorContext) {
        debug!(
            total_tracked_actors = self.actors.len(),
            "scheduler stopping",
        );

        let start_time = Instant::now();
        let stop_results =
            futures::future::join_all(self.actors.iter().map(|(id, actor)| async move {
                debug!(actor_id = actor.actor_id().as_ref(), "stopping actor");
                (id.clone(), actor.stop().await)
            }))
            .await;

        debug!(
            stopped_count = stop_results.len(),
            time_taken = format!("{:?}", start_time.elapsed()),
            "stopped all actors",
        );

        for stop_result in stop_results {
            debug!(
                actor_id = stop_result.0.as_ref(),
                successful = stop_result.1.is_ok(),
                "actor stopped",
            );
        }

        debug!("scheduler stopped");
    }
}

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

pub struct ActorCount;

impl Message for ActorCount {
    type Result = usize;
}

#[async_trait]
impl Handler<ActorCount> for ActorScheduler {
    async fn handle(&mut self, _: ActorCount, _ctx: &mut ActorContext) -> usize {
        self.actors.len()
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

#[cfg(feature = "remote")]
#[async_trait]
impl Handler<SetRemote> for ActorScheduler {
    async fn handle(&mut self, message: SetRemote, _ctx: &mut ActorContext) {
        self.remote = Some(message.0);
        trace!("actor scheduler is now configured for remoting");
    }
}

#[async_trait]
impl<A: Actor> Handler<RegisterActor<A>> for ActorScheduler
where
    A: 'static + Sync + Send,
{
    async fn handle(&mut self, message: RegisterActor<A>, _ctx: &mut ActorContext) {
        let actor_id = message.id;
        let previous_actor = self
            .actors
            .insert(actor_id.clone(), BoxedActorRef::from(message.actor_ref));

        if let Some(previous_actor) = previous_actor {
            warn!(
                previous_actor = previous_actor.actor_id().as_ref(),
                "actor replaced with a new reference and is no longer tracked by the scheduler"
            );
            return;
        }

        debug!(actor_id = actor_id.as_ref(), "actor registered");

        #[cfg(feature = "remote")]
        if let Some(remote) = &self.remote {
            debug!(
                node_id = remote.node_id(),
                actor_id = actor_id.as_ref(),
                "registering actor with remote registry",
            );

            remote.register_actor(actor_id, None);
        }
    }
}

#[async_trait]
impl Handler<DeregisterActor> for ActorScheduler {
    async fn handle(&mut self, msg: DeregisterActor, _ctx: &mut ActorContext) -> () {
        if let Some(_a) = self.actors.remove(&msg.0) {
            debug!("de-registered actor {}", msg.0);
        } else {
            warn!("actor {} not found to de-register", msg.0);
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
        let actor_ref = self
            .actors
            .get(&message.id)
            .and_then(|actor| actor.as_actor());

        #[cfg(feature = "remote")]
        if let Some(remote) = &self.remote {
            debug!(
                "[node={}] GetActor(actor_id={}) actor_found={}",
                remote.node_id(),
                &message.id,
                actor_ref.is_some()
            )
        } else {
            debug!(
                "[no-remote-attached] GetActor(actor_id={}) actor_found={}",
                &message.id,
                actor_ref.is_some()
            )
        }

        actor_ref
    }
}

pub fn start_actor<A: Actor>(
    actor: A,
    id: ActorId,
    actor_type: ActorType,
    on_start: Option<tokio::sync::oneshot::Sender<()>>,
    system: Option<ActorSystem>,
    parent_ref: Option<BoxedActorRef>,
    path: ActorPath,
) -> LocalActorRef<A>
where
    A: 'static + Send + Sync,
{
    let (tx, rx) = mpsc::unbounded_channel();
    let actor_ref = LocalActorRef::new(id, tx, path);
    let cloned_ref = actor_ref.clone();

    tokio::spawn(async move {
        ActorLoop::run(
            actor, actor_type, rx, on_start, cloned_ref, parent_ref, system,
        )
        .await;
    });

    actor_ref
}
