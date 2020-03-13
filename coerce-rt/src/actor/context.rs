use crate::actor::scheduler::{ActorScheduler, ActorType, GetActor, RegisterActor};
use crate::actor::{Actor, ActorId, ActorRef, ActorRefErr, BoxedActorRef};
use std::any::{Any, TypeId};
use std::collections::HashMap;

lazy_static! {
    static ref CURRENT_CONTEXT: ActorContext = { ActorContext::new() };
}

#[derive(Clone)]
pub struct ActorContext {
    scheduler: ActorRef<ActorScheduler>,
}

impl ActorContext {
    pub fn new() -> ActorContext {
        ActorContext {
            scheduler: ActorScheduler::new(),
        }
    }

    pub fn current_context() -> ActorContext {
        CURRENT_CONTEXT.clone()
    }

    pub async fn new_tracked_actor<A: Actor>(
        &mut self,
        actor: A,
    ) -> Result<ActorRef<A>, ActorRefErr>
        where
            A: 'static + Sync + Send,
    {
        self.new_actor(actor, ActorType::Tracked).await
    }

    pub async fn new_anon_actor<A: Actor>(&mut self, actor: A) -> Result<ActorRef<A>, ActorRefErr>
        where
            A: 'static + Sync + Send,
    {
        self.new_actor(actor, ActorType::Anonymous).await
    }

    pub async fn new_actor<A: Actor>(
        &mut self,
        actor: A,
        actor_type: ActorType,
    ) -> Result<ActorRef<A>, ActorRefErr>
        where
            A: 'static + Sync + Send,
    {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let actor_ref = self
            .scheduler
            .send(RegisterActor(actor, self.clone(), actor_type, tx))
            .await;

        match rx.await {
            Ok(true) => actor_ref,
            _ => Err(ActorRefErr::ActorUnavailable),
        }
    }

    pub async fn get_tracked_actor<A: Actor>(&mut self, id: ActorId) -> Option<ActorRef<A>>
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

pub struct ActorHandlerContext {
    id: ActorId,
    status: ActorStatus,
    boxed_ref: BoxedActorRef,
    core: Option<ActorContext>,
    attachments: HashMap<&'static str, BoxedAttachment>,
}

impl ActorHandlerContext {
    pub fn new(
        id: ActorId,
        core: Option<ActorContext>,
        status: ActorStatus,
        boxed_ref: BoxedActorRef,
        attachments: HashMap<&'static str, BoxedAttachment>,
    ) -> ActorHandlerContext {
        ActorHandlerContext {
            id,
            status,
            boxed_ref,
            core,
            attachments,
        }
    }

    pub fn id(&self) -> &ActorId {
        &self.id
    }

    pub fn core(&self) -> &ActorContext {
        if let Some(ctx) = &self.core {
            ctx
        } else {
            unreachable!()
        }
    }

    pub fn core_mut(&mut self) -> &mut ActorContext {
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

    pub(super) fn actor_ref<A: Actor>(&self) -> ActorRef<A>
        where
            A: 'static + Sync + Send,
    {
        ActorRef::from(self.boxed_ref.clone())
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
