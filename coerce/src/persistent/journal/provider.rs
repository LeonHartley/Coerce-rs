use crate::persistent::journal::storage::JournalStorageRef;
use std::sync::Arc;

pub trait StorageProvider: 'static + Send + Sync {
    fn journal_storage(&self) -> Option<JournalStorageRef>;
}

pub type StorageProviderRef = Arc<dyn StorageProvider>;

pub mod inmemory {
    use crate::persistent::journal::provider::StorageProvider;
    use crate::persistent::journal::storage::{JournalEntry, JournalStorage, JournalStorageRef};
    use parking_lot::RwLock;
    use std::collections::hash_map::Entry;
    use std::collections::HashMap;
    use std::mem;
    use std::sync::Arc;

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

        pub fn from_messages(messages: Vec<JournalEntry>) -> ActorJournal {
            ActorJournal {
                snapshots: vec![],
                messages,
            }
        }
    }

    #[derive(Default)]
    pub struct InMemoryJournalStorage {
        store: RwLock<HashMap<String, ActorJournal>>,
    }

    #[derive(Default)]
    pub struct InMemoryStorageProvider {
        store: Arc<InMemoryJournalStorage>,
    }

    impl InMemoryStorageProvider {
        pub fn new() -> InMemoryStorageProvider {
            Self::default()
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
            let mut store = self.store.write();
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
            let mut store = self.store.write();
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

        async fn write_message_batch(
            &self,
            persistence_id: &str,
            entries: Vec<JournalEntry>,
        ) -> anyhow::Result<()> {
            let mut store = self.store.write();
            if let Some(journal) = store.get_mut(persistence_id) {
                let mut entries = entries;
                journal.messages.append(&mut entries);
            } else {
                store.insert(
                    persistence_id.to_string(),
                    ActorJournal::from_messages(entries),
                );
            }

            Ok(())
        }

        async fn read_latest_snapshot(
            &self,
            persistence_id: &str,
        ) -> anyhow::Result<Option<JournalEntry>> {
            let store = self.store.read();
            Ok(store
                .get(persistence_id)
                .and_then(|j| j.snapshots.last().cloned()))
        }

        async fn read_latest_messages(
            &self,
            persistence_id: &str,
            from_sequence: i64,
        ) -> anyhow::Result<Option<Vec<JournalEntry>>> {
            let store = self.store.read();
            Ok(store.get(persistence_id).map(|journal| {
                let messages = match from_sequence {
                    0 => journal.messages.clone(),
                    from_sequence => {
                        let starting_message = journal
                            .messages
                            .iter()
                            .enumerate()
                            .find(|(_index, j)| j.sequence > from_sequence)
                            .map(|(index, _j)| index);

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

        async fn read_message(
            &self,
            persistence_id: &str,
            sequence_id: i64,
        ) -> anyhow::Result<Option<JournalEntry>> {
            let store = self.store.read();
            let journal = store.get(persistence_id);
            match journal {
                None => Ok(None),
                Some(journal) => Ok(journal
                    .messages
                    .iter()
                    .find(|n| n.sequence == sequence_id)
                    .cloned()),
            }
        }

        async fn read_messages(
            &self,
            persistence_id: &str,
            from_sequence: i64,
            to_sequence: i64,
        ) -> anyhow::Result<Option<Vec<JournalEntry>>> {
            let store = self.store.read();
            let journal = store.get(persistence_id);
            match journal {
                None => Ok(None),
                Some(journal) => {
                    if journal.messages.is_empty() {
                        Ok(None)
                    } else {
                        let first_seq = journal.messages.first().map(|m| m.sequence).unwrap();
                        let final_seq = journal.messages.last().map(|m| m.sequence).unwrap();

                        if to_sequence >= final_seq {
                            if first_seq >= from_sequence {
                                Ok(Some(journal.messages.clone()))
                            } else {
                                let starting_message = journal
                                    .messages
                                    .iter()
                                    .enumerate()
                                    .find(|(_index, j)| j.sequence > from_sequence)
                                    .map(|(index, _j)| index);

                                if let Some(starting_index) = starting_message {
                                    Ok(Some(
                                        journal.messages[starting_index..]
                                            .iter()
                                            .cloned()
                                            .collect(),
                                    ))
                                } else {
                                    Ok(Some(vec![]))
                                }
                            }
                        } else if first_seq >= from_sequence {
                            let end_message = journal
                                .messages
                                .iter()
                                .enumerate()
                                .find(|(_index, j)| j.sequence > from_sequence)
                                .map(|(index, _j)| index);

                            if let Some(end_index) = end_message {
                                Ok(Some(
                                    journal.messages[..end_index].iter().cloned().collect(),
                                ))
                            } else {
                                Ok(Some(vec![]))
                            }
                        } else {
                            panic!("todo: this")
                        }
                    }
                }
            }
        }

        async fn delete_messages_to(
            &self,
            persistence_id: &str,
            to_sequence: i64,
        ) -> anyhow::Result<()> {
            let mut store = self.store.write();
            let journal = store.entry(persistence_id.to_string());
            if let Entry::Occupied(mut journal) = journal {
                let journal = journal.get_mut();

                fn get_messages_to(
                    to_sequence: i64,
                    journal: &mut ActorJournal,
                ) -> Vec<JournalEntry> {
                    let starting_message = journal
                        .messages
                        .iter()
                        .enumerate()
                        .find(|(_index, j)| j.sequence >= to_sequence)
                        .map(|(index, _j)| index);

                    starting_message.map_or_else(|| vec![], |m| journal.messages.split_off(m))
                }

                let messages = if let Some(newest_msg) = journal.messages.last() {
                    if newest_msg.sequence < to_sequence {
                        vec![]
                    } else {
                        get_messages_to(to_sequence, journal)
                    }
                } else {
                    get_messages_to(to_sequence, journal)
                };

                *journal = ActorJournal {
                    snapshots: mem::take(&mut journal.snapshots),
                    messages,
                };
            }

            Ok(())
        }

        async fn delete_all(&self, persistence_id: &str) -> anyhow::Result<()> {
            let mut store = self.store.write();
            store.remove(persistence_id);
            Ok(())
        }
    }
}
