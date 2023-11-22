use crate::persistent::journal::proto::journal::JournalEntry as ProtoJournalEntry;
use anyhow::Result;
use protobuf::Message;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct JournalEntry {
    pub sequence: i64,
    pub payload_type: Arc<str>,
    pub bytes: Arc<Vec<u8>>,
}

#[async_trait]
pub trait JournalStorage: Send + Sync {
    // TODO: add the ability to limit the maximum size of snapshots,
    //       loading these back could cause an unexpected OOM
    async fn write_snapshot(&self, persistence_id: &str, entry: JournalEntry) -> Result<()>;

    async fn write_message(&self, persistence_id: &str, entry: JournalEntry) -> Result<()>;

    async fn write_message_batch(
        &self,
        persistence_id: &str,
        entries: Vec<JournalEntry>,
    ) -> Result<()>;

    async fn read_latest_snapshot(&self, persistence_id: &str) -> Result<Option<JournalEntry>>;

    // TODO: add the ability to stream the messages, rather than load all up front,
    //       if the actor has a very large journal, this could cause an unexpected OOM.
    //       payload size limits should also be applied.
    async fn read_latest_messages(
        &self,
        persistence_id: &str,
        from_sequence: i64,
    ) -> Result<Option<Vec<JournalEntry>>>;

    async fn read_message(
        &self,
        persistence_id: &str,
        sequence_id: i64,
    ) -> Result<Option<JournalEntry>>;

    async fn read_messages(
        &self,
        persistence_id: &str,
        from_sequence: i64,
        to_sequence: i64,
    ) -> Result<Option<Vec<JournalEntry>>>;

    async fn delete_messages_to(&self, persistence_id: &str, to_sequence: i64) -> Result<()>;

    async fn delete_all(&self, persistence_id: &str) -> Result<()>;
}

pub type JournalStorageRef = Arc<dyn JournalStorage>;

impl JournalEntry {
    pub fn read_from_slice(data: &[u8]) -> Option<Self> {
        let journal_entry = ProtoJournalEntry::parse_from_bytes(data);
        if let Ok(journal_entry) = journal_entry {
            Some(JournalEntry {
                sequence: journal_entry.sequence,
                payload_type: journal_entry.payload_type.into(),
                bytes: Arc::new(journal_entry.bytes),
            })
        } else {
            None
        }
    }

    pub fn read_from_bytes(data: Vec<u8>) -> Option<Self> {
        Self::read_from_slice(data.as_slice())
    }

    pub fn write_to_bytes(&self) -> Option<Vec<u8>> {
        let journal_entry = self;
        let proto = ProtoJournalEntry {
            sequence: journal_entry.sequence,
            payload_type: journal_entry.payload_type.to_string(),
            bytes: journal_entry.bytes.as_ref().clone(),
            ..Default::default()
        };

        let bytes = proto.write_to_bytes();
        if let Ok(bytes) = bytes {
            Some(bytes)
        } else {
            None
        }
    }
}
