pub mod actor;
pub mod context;
pub mod journal;

pub use actor::*;

use crate::actor::context::ActorContext;
use crate::actor::system::ActorSystem;
use crate::persistent::context::ActorPersistence;
use crate::persistent::journal::provider::{StorageProvider, StorageProviderRef};
use std::sync::Arc;

#[derive(Clone)]
pub struct Persistence {
    provider: StorageProviderRef,
}

impl Persistence {
    pub fn from<S: StorageProvider>(provider: S) -> Persistence {
        let provider = Arc::new(provider);
        Persistence { provider }
    }

    pub fn provider(&self) -> &StorageProviderRef {
        &self.provider
    }
}

pub trait ConfigurePersistence {
    fn add_persistence(self, persistence: Persistence) -> Self;
}

impl ConfigurePersistence for ActorSystem {
    fn add_persistence(mut self, persistence: Persistence) -> Self {
        self.set_persistence(Some(persistence));

        self
    }
}
