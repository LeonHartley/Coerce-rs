use crate::actor::scheduler::{start_actor, ActorScheduler, ActorType, GetActor, RegisterActor};
use crate::actor::{new_actor_id, Actor, ActorId, ActorRefErr, LocalActorRef};
use crate::remote::system::RemoteActorSystem;
use std::sync::Arc;
use uuid::Uuid;

lazy_static! {
    static ref CURRENT_SYSTEM: ActorSystem = ActorSystem::new();
}

#[derive(Clone)]
pub struct ActorSystem {
    system_id: Uuid,
    scheduler: LocalActorRef<ActorScheduler>,
    remote: Option<Arc<RemoteActorSystem>>,
}

impl ActorSystem {
    pub fn new() -> ActorSystem {
        ActorSystem {
            system_id: Uuid::new_v4(),
            scheduler: ActorScheduler::new(),
            remote: None,
        }
    }

    pub fn system_id(&self) -> &Uuid {
        &self.system_id
    }

    pub fn scheduler(&self) -> LocalActorRef<ActorScheduler> {
        self.scheduler.clone()
    }

    pub fn current_system() -> ActorSystem {
        CURRENT_SYSTEM.clone()
    }

    pub fn set_remote(&mut self) {}

    pub fn remote(&self) -> &RemoteActorSystem {
        self.remote
            .as_ref()
            .expect("this ActorSystem is not setup for remoting")
    }

    pub fn remote_owned(&mut self) -> RemoteActorSystem {
        self.remote
            .as_ref()
            .map(|s| s.as_ref().clone())
            .expect("this ActorSystem is not setup for remoting")
    }

    pub async fn new_tracked_actor<A: Actor>(
        &mut self,
        actor: A,
    ) -> Result<LocalActorRef<A>, ActorRefErr>
    where
        A: 'static + Sync + Send,
    {
        let id = new_actor_id();
        self.new_actor(id, actor, ActorType::Tracked).await
    }

    pub async fn new_anon_actor<A: Actor>(
        &mut self,
        actor: A,
    ) -> Result<LocalActorRef<A>, ActorRefErr>
    where
        A: 'static + Sync + Send,
    {
        let id = new_actor_id();
        self.new_actor(id, actor, ActorType::Anonymous).await
    }

    pub async fn new_actor<A: Actor>(
        &mut self,
        id: ActorId,
        actor: A,
        actor_type: ActorType,
    ) -> Result<LocalActorRef<A>, ActorRefErr>
    where
        A: 'static + Sync + Send,
    {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let actor_ref = start_actor(
            actor,
            id.clone(),
            actor_type,
            Some(tx),
            if actor_type.is_tracked() {
                Some(self.scheduler.clone())
            } else {
                None
            },
            Some(self.clone()),
        );

        if actor_type.is_tracked() {
            let _ = self.scheduler.notify(RegisterActor {
                id,
                actor_ref: actor_ref.clone(),
            });
        }

        match rx.await {
            Ok(true) => Ok(actor_ref),
            _ => Err(ActorRefErr::ActorUnavailable),
        }
    }

    pub async fn get_tracked_actor<A: Actor>(&mut self, id: ActorId) -> Option<LocalActorRef<A>>
    where
        A: 'static + Sync + Send,
    {
        match self.scheduler.send(GetActor::new(id)).await {
            Ok(a) => a,
            Err(_) => None,
        }
    }
}
