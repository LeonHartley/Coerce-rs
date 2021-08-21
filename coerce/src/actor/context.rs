use crate::actor::children::Children;
use crate::actor::system::ActorSystem;
use crate::actor::{Actor, ActorId, BoxedActorRef, CoreActorRef, LocalActorRef};
use crate::persistent::context::ActorPersistence;

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
    children: Option<Children>,
    system: Option<ActorSystem>,
}

impl Drop for ActorContext {
    fn drop(&mut self) {
        match self.status {
            ActorStatus::Starting => {
                error!("actor panicked while starting");
            }
            ActorStatus::Started => {
                if self.system.is_none() || self.system().is_terminated() {
                    trace!(
                        "actor (id={}) has stopped due to system shutdown",
                        &self.boxed_ref.actor_id()
                    );
                } else {
                    warn!(
                        "actor (id={}) has stopped unexpectedly",
                        &self.boxed_ref.actor_id()
                    );
                }
            }
            ActorStatus::Stopping => {
                error!("actor panicked while stopping");
            }
            ActorStatus::Stopped => {
                trace!(
                    "actor (id={}) stopped, context dropped",
                    &self.boxed_ref.actor_id()
                );
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
            children: None,
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

    pub(crate) fn actor_ref<A: Actor>(&self) -> LocalActorRef<A> {
        (&self.boxed_ref.0)
            .as_any()
            .downcast_ref::<LocalActorRef<A>>()
            .expect("actor_ref")
            .clone()
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

    pub fn new_child<A: Actor>(_id: &ActorId, _actor: A) {}
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
