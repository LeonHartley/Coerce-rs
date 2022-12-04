use crate::persistent::journal::proto::journal::JournalEntry as ProtoJournalEntry;
use anyhow::Result;
use protobuf::Message;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct JournalEntry {
    pub sequence: i64,
    pub payload_type: String,
    pub bytes: Arc<Vec<u8>>,
}

#[async_trait]
pub trait JournalStorage: Send + Sync {
    async fn write_snapshot(&self, persistence_id: &str, entry: JournalEntry) -> Result<()>;

    async fn write_message(&self, persistence_id: &str, entry: JournalEntry) -> Result<()>;

    async fn read_latest_snapshot(&self, persistence_id: &str) -> Result<Option<JournalEntry>>;

    async fn read_latest_messages(
        &self,
        persistence_id: &str,
        from_sequence: i64,
    ) -> Result<Option<Vec<JournalEntry>>>;

    async fn delete_all(&self, persistence_id: &str) -> Result<()>;
}

pub type JournalStorageRef = Arc<dyn JournalStorage>;

impl JournalEntry {
    pub fn read_from_bytes(data: Vec<u8>) -> Option<Self> {
        let journal_entry = ProtoJournalEntry::parse_from_bytes(&data);
        if let Ok(journal_entry) = journal_entry {
            Some(JournalEntry {
                sequence: journal_entry.sequence,
                payload_type: journal_entry.payload_type,
                bytes: Arc::new(journal_entry.bytes),
            })
        } else {
            None
        }
    }

    pub fn write_to_bytes(&self) -> Option<Vec<u8>> {
        let journal_entry = self;
        let proto = ProtoJournalEntry {
            sequence: journal_entry.sequence,
            payload_type: journal_entry.payload_type.clone(),
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
