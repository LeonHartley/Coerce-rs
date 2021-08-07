use crate::actor::scheduler::{start_actor, ActorScheduler, ActorType, GetActor, RegisterActor};
use crate::actor::{new_actor_id, Actor, ActorId, ActorRefErr, LocalActorRef};
use crate::remote::system::RemoteActorSystem;

use uuid::Uuid;

lazy_static! {
    static ref CURRENT_SYSTEM: ActorSystem = ActorSystem::new();
}

#[derive(Clone)]
pub struct ActorSystem {
    system_id: Uuid,
    scheduler: LocalActorRef<ActorScheduler>,
    remote: Option<RemoteActorSystem>,
}

impl ActorSystem {
    pub fn new() -> ActorSystem {
        let system_id = Uuid::new_v4();
        ActorSystem {
            system_id,
            scheduler: ActorScheduler::new(system_id),
            remote: None,
        }
    }

    pub fn system_id(&self) -> &Uuid {
        &self.system_id
    }

    pub fn scheduler(&self) -> &LocalActorRef<ActorScheduler> {
        &self.scheduler
    }

    pub fn current_system() -> ActorSystem {
        CURRENT_SYSTEM.clone()
    }

    pub fn set_remote(&mut self, remote: RemoteActorSystem) {
        self.remote = Some(remote);
    }

    pub fn remote(&self) -> &RemoteActorSystem {
        self.remote
            .as_ref()
            .expect("this ActorSystem is not setup for remoting")
    }

    pub fn remote_owned(&self) -> RemoteActorSystem {
        self.remote
            .as_ref()
            .map(|s| s.clone())
            .expect("this ActorSytem is not setup for remoting")
    }

    pub fn is_remote(&self) -> bool {
        self.remote.is_some()
    }

    pub async fn new_tracked_actor<A: Actor>(
        &self,
        actor: A,
    ) -> Result<LocalActorRef<A>, ActorRefErr> {
        let id = new_actor_id();
        self.new_actor(id, actor, ActorType::Tracked).await
    }

    pub async fn new_anon_actor<A: Actor>(
        &self,
        actor: A,
    ) -> Result<LocalActorRef<A>, ActorRefErr> {
        let id = new_actor_id();
        self.new_actor(id, actor, ActorType::Anonymous).await
    }

    pub async fn new_actor<A: Actor>(
        &self,
        id: ActorId,
        actor: A,
        actor_type: ActorType,
    ) -> Result<LocalActorRef<A>, ActorRefErr> {
        let actor_type_name = A::type_name();
        let span = tracing::trace_span!(
            "ActorSystem::new_actor",
            actor_type = match actor_type {
                ActorType::Anonymous => "Anonymous",
                _ => "Tracked",
            },
            actor_type_name = actor_type_name,
            actor_id = id.as_str(),
        );

        let _enter = span.enter();

        let (tx, rx) = tokio::sync::oneshot::channel();
        let actor_ref = start_actor(
            actor,
            id.clone(),
            actor_type,
            Some(tx),
            Some(self.clone()),
            None,
        );

        if actor_type.is_tracked() {
            let _ = self
                .scheduler
                .send(RegisterActor {
                    id: id.clone(),
                    actor_ref: actor_ref.clone(),
                })
                .await;
        }

        match rx.await {
            Ok(true) => Ok(actor_ref),
            _ => Err(ActorRefErr::ActorUnavailable),
        }
    }

    pub async fn get_tracked_actor<A: Actor>(&self, id: ActorId) -> Option<LocalActorRef<A>> {
        let actor_type_name = A::type_name();
        let span = tracing::trace_span!(
            "ActorSystem::get_tracked_actor",
            actor_id = id.as_str(),
            actor_type_name
        );
        let _enter = span.enter();

        match self.scheduler.send(GetActor::new(id)).await {
            Ok(a) => a,
            Err(_) => None,
        }
    }
}
