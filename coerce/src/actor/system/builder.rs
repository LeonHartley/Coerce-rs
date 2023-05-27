use crate::actor::scheduler::ActorScheduler;
use crate::actor::system::{ActorSystem, ActorSystemCore};
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::Arc;
use uuid::Uuid;

#[cfg(feature = "persistence")]
use crate::persistent::{provider::StorageProvider, Persistence};

#[derive(Default)]
pub struct ActorSystemBuilder {
    system_id: Option<Uuid>,
    system_name: Option<String>,

    #[cfg(feature = "persistence")]
    persistence: Option<Arc<Persistence>>,
}

impl ActorSystemBuilder {
    pub fn system_name(mut self, name: impl ToString) -> Self {
        self.system_name = Some(name.to_string());
        self
    }

    #[cfg(feature = "persistence")]
    pub fn with_persistence<S: StorageProvider>(mut self, provider: S) -> Self {
        self.persistence = Some(Persistence::from(provider).into());
        self
    }

    pub fn build(self) -> ActorSystem {
        let system_id = self.system_id.unwrap_or_else(|| Uuid::new_v4());
        let system_name: Arc<str> = self.system_name.map_or_else(
            || {
                std::env::var("COERCE_ACTOR_SYSTEM").map_or_else(
                    |_e| format!("{}", system_id).into(),
                    |v| v.to_string().into(),
                )
            },
            |s| s.into(),
        );

        let scheduler = ActorScheduler::new(system_id, system_name.clone());
        ActorSystem {
            core: Arc::new(ActorSystemCore {
                system_id,
                system_name,
                scheduler,
                is_terminated: Arc::new(AtomicBool::new(false)),
                context_counter: Arc::new(AtomicU64::new(1)),

                #[cfg(feature = "persistence")]
                persistence: self.persistence,

                #[cfg(feature = "remote")]
                remote: None,
            }),
        }
    }
}
