use crate::actor::context::ActorContext;
use crate::actor::message::Message;

use crate::persistent::types::JournalTypes;
use crate::persistent::{PersistentActor, Recover};
use std::sync::Arc;

#[derive(Clone)]
pub struct EventBatch<A: PersistentActor> {
    journal_types: Arc<JournalTypes<A>>,
    entries: Vec<BatchedEntry>,
}

#[derive(Clone)]
pub struct BatchedEntry {
    pub payload_type: Arc<str>,
    pub bytes: Arc<Vec<u8>>,
}

impl<A: PersistentActor> EventBatch<A> {
    pub fn create(ctx: &ActorContext) -> Self {
        let journal = ctx.persistence().journal::<A>();
        let journal_types = journal.get_types();

        Self {
            journal_types,
            entries: vec![],
        }
    }

    pub fn message<M: Message>(&mut self, message: M) -> &mut Self
    where
        A: Recover<M>,
    {
        let payload_type = self.journal_types.message_type_mapping::<M>().unwrap();
        let entry = BatchedEntry {
            payload_type,
            bytes: message.as_bytes().unwrap().into(),
        };

        self.entries.push(entry);
        self
    }

    pub fn entries(&self) -> &Vec<BatchedEntry> {
        &self.entries
    }
}
