use crate::persistent::journal::provider::StorageProviderRef;
use crate::persistent::journal::Journal;
use crate::persistent::PersistentActor;
use std::any::Any;

pub struct ActorPersistence {
    storage_provider: StorageProviderRef,
    journal: Option<BoxedJournal>,
}

type BoxedJournal = Box<dyn Any + Sync + Send>;

impl ActorPersistence {
    pub fn new(storage_provider: StorageProviderRef) -> Self {
        Self {
            storage_provider,
            journal: None,
        }
    }

    pub async fn init_journal<A: PersistentActor>(
        &mut self,
        persistence_id: String,
    ) -> &mut Journal<A> {
        let storage = self
            .storage_provider
            .journal_storage()
            .expect("journal storage not configured");

        let journal = Journal::<A>::new(persistence_id, storage).await;
        self.journal = Some(Box::new(journal));
        self.journal.as_mut().unwrap().downcast_mut().unwrap()
    }

    pub fn journal<A: PersistentActor>(&self) -> &Journal<A> {
        self.journal
            .as_ref()
            .expect("journal not initialised")
            .downcast_ref()
            .unwrap()
    }
    pub fn journal_mut<A: PersistentActor>(&mut self) -> &mut Journal<A> {
        self.journal
            .as_mut()
            .expect("journal not initialised")
            .downcast_mut()
            .unwrap()
    }
}
