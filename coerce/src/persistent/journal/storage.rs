use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct JournalEntry {
    pub sequence: i64,
    pub payload_type: String,
    pub bytes: Vec<u8>,
}

#[async_trait]
pub trait JournalStorage: Send + Sync {
    async fn write_snapshot(&self, persistence_id: &str, entry: JournalEntry);

    async fn write_message(&self, persistence_id: &str, entry: JournalEntry);

    async fn read_latest_snapshot(&self, persistence_id: &str) -> Option<JournalEntry>;

    async fn read_latest_messages(
        &self,
        persistence_id: &str,
        from_sequence: i64,
    ) -> Option<Vec<JournalEntry>>;
}

pub type JournalStorageRef = Arc<dyn JournalStorage>;
