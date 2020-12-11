use crate::actor::scheduler::{ActorScheduler, ActorType, GetActor, RegisterActor};
use crate::actor::{Actor, ActorId, ActorRefErr, BoxedActorRef, LocalActorRef};
use crate::remote::system::RemoteActorSystem;
use std::any::Any;
use std::collections::HashMap;
use uuid::Uuid;

lazy_static! {
    static ref CURRENT_SYSTEM: ActorSystem = ActorSystem::new();
}

#[derive(Clone)]
pub struct ActorSystem {
    system_id: Uuid,
    scheduler: LocalActorRef<ActorScheduler>,
    remote: Option<Box<RemoteActorSystem>>,
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

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum ActorStatus {
    Starting,
    Started,
    Stopping,
    Stopped,
}

type BoxedAttachment = Box<dyn Any + 'static + Sync + Send>;

pub struct ActorContext {
    boxed_ref: BoxedActorRef,
    status: ActorStatus,
    core: Option<ActorSystem>,
    children: Vec<BoxedActorRef>,
    attachments: HashMap<&'static str, BoxedAttachment>,
}

impl ActorContext {
    pub fn new(
        core: Option<ActorSystem>,
        status: ActorStatus,
        boxed_ref: BoxedActorRef,
        attachments: HashMap<&'static str, BoxedAttachment>,
    ) -> ActorContext {
        ActorContext {
            boxed_ref,
            status,
            core,
            attachments,
            children: vec![],
        }
    }

    pub fn id(&self) -> &ActorId {
        &self.boxed_ref.id
    }

    pub fn system(&self) -> &ActorSystem {
        if let Some(ctx) = &self.core {
            ctx
        } else {
            unreachable!()
        }
    }

    pub fn core_mut(&mut self) -> &mut ActorSystem {
        if let Some(ctx) = &mut self.core {
            ctx
        } else {
            unreachable!()
        }
    }

    pub fn set_status(&mut self, state: ActorStatus) {
        self.status = state
    }

    pub fn get_status(&self) -> &ActorStatus {
        &self.status
    }

    pub(super) fn actor_ref<A: Actor>(&self) -> LocalActorRef<A>
    where
        A: 'static + Sync + Send,
    {
        LocalActorRef::from(self.boxed_ref.clone())
    }

    pub fn add_attachment<T: Any>(&mut self, key: &'static str, attachment: T)
    where
        T: 'static + Send + Sync,
    {
        self.attachments.insert(key, Box::new(attachment));
    }

    pub fn attachment<T: Any>(&self, key: &str) -> Option<&T> {
        if let Some(attachment) = self.attachments.get(key) {
            attachment.downcast_ref()
        } else {
            None
        }
    }

    pub fn attachment_mut<T: Any>(&mut self, key: &str) -> Option<&mut T> {
        if let Some(attachment) = self.attachments.get_mut(key) {
            attachment.downcast_mut()
        } else {
            None
        }
    }
}
