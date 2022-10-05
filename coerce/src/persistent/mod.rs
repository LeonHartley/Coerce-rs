pub mod actor;
pub mod context;
pub mod inspect;
pub mod journal;

pub use actor::*;
use std::any::{Any, TypeId};
use std::collections::HashMap;

use crate::actor::system::ActorSystem;

use crate::actor::Actor;
use crate::persistent::journal::provider::{StorageProvider, StorageProviderRef};
use std::sync::Arc;

#[derive(Clone)]
pub struct Persistence {
    default_provider: StorageProviderRef,
    actor_type_specific_providers: HashMap<TypeId, StorageProviderRef>,
}

impl Persistence {
    pub fn from<S: StorageProvider>(provider: S) -> Persistence {
        let default_provider = Arc::new(provider);
        Persistence {
            default_provider,
            actor_type_specific_providers: HashMap::new(),
        }
    }

    pub fn actor_provider<A: PersistentActor, S: StorageProvider>(mut self, provider: S) -> Self {
        let actor_type = TypeId::of::<A>();
        self.actor_type_specific_providers
            .insert(actor_type, Arc::new(provider));
        self
    }

    pub fn provider(&self, actor_type_id: TypeId) -> StorageProviderRef {
        self.actor_type_specific_providers
            .get(&actor_type_id)
            .map_or_else(|| self.default_provider.clone(), |s| s.clone())
    }
}
