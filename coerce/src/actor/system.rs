use crate::actor::scheduler::{ActorScheduler, ActorType, GetActor, RegisterActor};
use crate::actor::{Actor, ActorId, ActorRefErr, LocalActorRef};
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

    pub fn current_system() -> ActorSystem {
        CURRENT_SYSTEM.clone()
    }

    fn remote(&self) -> &RemoteActorSystem {
        self.remote
            .as_ref()
            .expect("this ActorSystem is not setup for remoting")
    }

    fn remote_mut(&mut self) -> &RemoteActorSystem {
        self.remote
            .as_mut()
            .expect("this ActorSystem is not setup for remoting")
    }

    pub async fn new_tracked_actor<A: Actor>(
        &mut self,
        actor: A,
    ) -> Result<LocalActorRef<A>, ActorRefErr>
    where
        A: 'static + Sync + Send,
    {
        let id = format!("{}", Uuid::new_v4());
        self.new_actor(id, actor, ActorType::Tracked).await
    }

    pub async fn new_anon_actor<A: Actor>(
        &mut self,
        actor: A,
    ) -> Result<LocalActorRef<A>, ActorRefErr>
    where
        A: 'static + Sync + Send,
    {
        let id = format!("{}", Uuid::new_v4());
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
        let actor_ref = self
            .scheduler
            .send(RegisterActor {
                id,
                actor,
                system: self.clone(),
                actor_type,
                start_rx: tx,
            })
            .await;

        match rx.await {
            Ok(true) => actor_ref,
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
