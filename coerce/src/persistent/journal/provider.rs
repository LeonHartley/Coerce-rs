use crate::persistent::journal::storage::{JournalStorage, JournalStorageRef};
use std::sync::Arc;

pub trait StorageProvider: 'static + Send + Sync {
    fn journal_storage(&self) -> Option<JournalStorageRef>;
}

pub type StorageProviderRef = Arc<dyn StorageProvider>;

pub mod inmemory {
    use crate::persistent::journal::provider::{StorageProvider, StorageProviderRef};
    use crate::persistent::journal::storage::{JournalEntry, JournalStorage, JournalStorageRef};
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    #[derive(Debug)]
    struct ActorJournal {
        snapshots: Vec<JournalEntry>,
        messages: Vec<JournalEntry>,
    }

    impl ActorJournal {
        pub fn from_snapshot(entry: JournalEntry) -> ActorJournal {
            ActorJournal {
                snapshots: vec![entry],
                messages: vec![],
            }
        }

        pub fn from_message(entry: JournalEntry) -> ActorJournal {
            ActorJournal {
                snapshots: vec![],
                messages: vec![entry],
            }
        }
    }

    pub struct InMemoryJournalStorage {
        store: RwLock<HashMap<String, ActorJournal>>,
    }

    pub struct InMemoryStorageProvider {
        store: Arc<InMemoryJournalStorage>,
    }

    impl InMemoryStorageProvider {
        pub fn new() -> InMemoryStorageProvider {
            InMemoryStorageProvider {
                store: Arc::new(InMemoryJournalStorage {
                    store: RwLock::new(HashMap::new()),
                }),
            }
        }
    }

    impl StorageProvider for InMemoryStorageProvider {
        fn journal_storage(&self) -> Option<JournalStorageRef> {
            Some(self.store.clone())
        }
    }

    #[async_trait]
    impl JournalStorage for InMemoryJournalStorage {
        async fn write_snapshot(
            &self,
            persistence_id: &str,
            entry: JournalEntry,
        ) -> anyhow::Result<()> {
            let mut store = self.store.write().await;
            if let Some(journal) = store.get_mut(persistence_id) {
                journal.snapshots.push(entry);
            } else {
                store.insert(
                    persistence_id.to_string(),
                    ActorJournal::from_snapshot(entry),
                );
            }

            Ok(())
        }

        async fn write_message(
            &self,
            persistence_id: &str,
            entry: JournalEntry,
        ) -> anyhow::Result<()> {
            let mut store = self.store.write().await;
            if let Some(journal) = store.get_mut(persistence_id) {
                journal.messages.push(entry);
            } else {
                store.insert(
                    persistence_id.to_string(),
                    ActorJournal::from_message(entry),
                );
            }

            Ok(())
        }

        async fn read_latest_snapshot(
            &self,
            persistence_id: &str,
        ) -> anyhow::Result<Option<JournalEntry>> {
            let store = self.store.read().await;

            Ok(store
                .get(persistence_id)
                .and_then(|j| j.snapshots.last().cloned()))
        }

        async fn read_latest_messages(
            &self,
            persistence_id: &str,
            from_sequence: i64,
        ) -> anyhow::Result<Option<Vec<JournalEntry>>> {
            let store = self.store.read().await;
            Ok(store.get(persistence_id).map(|journal| {
                let messages = match from_sequence {
                    0 => journal.messages.clone(),
                    from_sequence => {
                        let starting_message = journal
                            .messages
                            .iter()
                            .enumerate()
                            .find(|(index, j)| j.sequence > from_sequence)
                            .map(|(index, j)| index);

                        if let Some(starting_index) = starting_message {
                            journal.messages[starting_index..].iter().cloned().collect()
                        } else {
                            vec![]
                        }
                    }
                };

                trace!(
                    "storage found {} messages for persistence_id={}, from_sequence={}",
                    messages.len(),
                    persistence_id,
                    from_sequence
                );

                messages
            }))
        }

        async fn delete_all(&self, persistence_id: &str) -> anyhow::Result<()> {
            let mut store = self.store.write().await;
            store.remove(persistence_id);
            Ok(())
        }
    }
}
