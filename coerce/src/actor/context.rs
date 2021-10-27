use crate::actor::system::ActorSystem;
use crate::actor::{Actor, ActorId, ActorRefErr, BoxedActorRef, CoreActorRef, LocalActorRef};
use crate::persistent::context::ActorPersistence;

use crate::actor::supervised::Supervised;
use std::any::{Any, TypeId};
use std::collections::HashMap;

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum ActorStatus {
    Starting,
    Started,
    Stopping,
    Stopped,
}

pub struct ActorContext {
    status: ActorStatus,
    persistence: Option<ActorPersistence>,
    boxed_ref: BoxedActorRef,
    boxed_parent_ref: Option<BoxedActorRef>,
    supervised: Option<Supervised>,
    system: Option<ActorSystem>,
}

impl Drop for ActorContext {
    fn drop(&mut self) {
        if let Some(boxed_parent_ref) = &self.boxed_parent_ref {
            boxed_parent_ref.notify_child_terminated(self.id().into());
            info!("notify child terminated");
        }

        if let Some(supervised) = &self.supervised {}

        match self.status {
            ActorStatus::Starting => {
                error!("actor panicked while starting");
            }
            ActorStatus::Started => {
                if self.system.is_some() && self.system().is_terminated() {
                    trace!(
                        "actor (id={}) has stopped due to system shutdown",
                        &self.id()
                    );
                } else {
                    warn!("actor (id={}) has stopped unexpectedly", &self.id());
                }
            }
            ActorStatus::Stopping => {
                error!("actor panicked while stopping");
            }
            ActorStatus::Stopped => {
                trace!("actor (id={}) stopped, context dropped", &self.id());
            }
        }
    }
}

impl ActorContext {
    pub fn new(
        system: Option<ActorSystem>,
        status: ActorStatus,
        boxed_ref: BoxedActorRef,
    ) -> ActorContext {
        ActorContext {
            boxed_ref,
            status,
            system,
            supervised: None,
            persistence: None,
            boxed_parent_ref: None,
        }
    }

    pub fn id(&self) -> &ActorId {
        &self.boxed_ref.actor_id()
    }

    pub fn system(&self) -> &ActorSystem {
        if let Some(system) = &self.system {
            system
        } else {
            unreachable!()
        }
    }

    pub fn system_mut(&mut self) -> &mut ActorSystem {
        if let Some(system) = &mut self.system {
            system
        } else {
            unreachable!()
        }
    }

    pub fn set_system(&mut self, system: ActorSystem) {
        self.system = Some(system);
    }

    pub fn set_status(&mut self, state: ActorStatus) {
        self.status = state
    }

    pub fn get_status(&self) -> &ActorStatus {
        &self.status
    }

    pub fn is_starting(&self) -> bool {
        self.status == ActorStatus::Starting
    }

    pub fn actor_ref<A: Actor>(&self) -> LocalActorRef<A> {
        (&self.boxed_ref.0)
            .as_any()
            .downcast_ref::<LocalActorRef<A>>()
            .expect("actor_ref")
            .clone()
    }

    pub fn boxed_actor_ref(&self) -> BoxedActorRef {
        self.boxed_ref.clone()
    }

    pub fn child_ref<A: Actor>(&self, id: &ActorId) -> Option<LocalActorRef<A>> {
        self.supervised.as_ref().and_then(|s| s.child(id))
    }

    pub fn boxed_child_ref(&self, id: &ActorId) -> Option<BoxedActorRef> {
        self.supervised.as_ref().and_then(|s| s.child_boxed(id))
    }

    pub fn persistence(&self) -> &ActorPersistence {
        self.persistence
            .as_ref()
            .expect("ctx is not setup for persistence")
    }

    pub fn persistence_mut(&mut self) -> &mut ActorPersistence {
        self.persistence
            .as_mut()
            .expect("ctx is not setup for persistence")
    }

    pub fn set_persistence(&mut self, persistence: ActorPersistence) {
        self.persistence = Some(persistence);
    }

    pub fn supervised_mut(&mut self) -> Option<&mut Supervised> {
        self.supervised.as_mut()
    }

    pub async fn spawn<A: Actor>(
        &mut self,
        id: ActorId,
        actor: A,
    ) -> Result<LocalActorRef<A>, ActorRefErr> {
        let supervised = {
            if self.supervised.is_none() {
                self.supervised = Some(Supervised::new());
            }

            self.supervised.as_mut().unwrap()
        };

        let system = self.system.as_ref().unwrap().clone();
        let parent_ref = self.boxed_ref.clone();
        supervised.spawn(id, actor, system, parent_ref).await
    }

    pub fn with_parent(mut self, boxed_parent_ref: Option<BoxedActorRef>) -> Self {
        self.boxed_parent_ref = boxed_parent_ref;
        self
    }
}

impl ActorContext {
    pub fn with_persistence(self) -> Self {
        let mut ctx = self;
        let persistence = ctx
            .system()
            .persistence()
            .expect("persistence not configured");

        ctx.persistence = Some(ActorPersistence::new(persistence.provider()));

        ctx
    }
}
