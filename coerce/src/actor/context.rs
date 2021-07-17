use crate::actor::system::ActorSystem;
use crate::actor::{Actor, ActorId, BoxedActorRef, LocalActorRef};

use crate::actor::children::Children;
use std::any::Any;
use std::collections::HashMap;

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
    boxed_parent_ref: Option<BoxedActorRef>,
    status: ActorStatus,
    core: Option<ActorSystem>,
    children: Option<Children>,
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
            children: None,
            boxed_parent_ref: None,
        }
    }

    pub fn id(&self) -> &ActorId {
        &self.boxed_ref.0
    }

    pub fn system(&self) -> &ActorSystem {
        if let Some(ctx) = &self.core {
            ctx
        } else {
            unreachable!()
        }
    }

    pub fn system_mut(&mut self) -> &mut ActorSystem {
        if let Some(ctx) = &mut self.core {
            ctx
        } else {
            unreachable!()
        }
    }

    pub fn set_system(&mut self, system: ActorSystem) {
        self.core = Some(system);
    }

    pub fn set_status(&mut self, state: ActorStatus) {
        self.status = state
    }

    pub fn get_status(&self) -> &ActorStatus {
        &self.status
    }

    pub(crate) fn actor_ref<A: Actor>(&self) -> LocalActorRef<A>
    where
        A: 'static + Sync + Send,
    {
        self.boxed_ref
            .1
            .downcast_ref::<LocalActorRef<A>>()
            .expect("actor_ref")
            .clone()
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

    pub fn new_child<A: Actor>(id: &ActorId, actor: A) {}
}
