use coerce::actor::system::ActorSystem;
use coerce::persistent::journal::storage::JournalEntry;
use coerce::persistent::provider::StorageProvider;
use coerce::persistent::storage::JournalStorageRef;
use coerce_redis::journal::{RedisStorageConfig, RedisStorageProvider};
use tracing::Level;
use tracing_subscriber::fmt;

pub struct RedisTest {
    pub system: ActorSystem,
    pub storage: JournalStorageRef,
}

impl RedisTest {
    pub async fn new(cluster: bool, key_prefix: &str) -> Self {
        let _ = fmt()
            .with_file(true)
            .with_line_number(true)
            .with_target(true)
            .with_thread_names(true)
            .with_max_level(Level::TRACE)
            .try_init();

        let system = ActorSystem::new();

        let nodes = if cluster {
            vec![
                "redis://127.0.0.1:7000/".to_string(),
            ]
        } else {
            vec![
                "redis://127.0.0.1:6379/".to_string()
            ]
        };

        let provider = RedisStorageProvider::connect(
            RedisStorageConfig {
                nodes,
                key_prefix: key_prefix.to_string(),
                cluster,
                use_key_hashtags: true,
            },
            &system,
        )
        .await;

        let storage = provider.journal_storage().expect("journal storage");
        system.to_persistent(provider.into());

        Self { system, storage }
    }

    pub async fn test_snapshot_read_write(&self) {
        let persistence_id = "hi";
        let entries = generate_entries(3);

        for entry in entries {
            self.storage
                .write_snapshot(persistence_id, entry)
                .await
                .expect("write snapshot");
        }

        let latest_snapshot = self.storage.read_latest_snapshot(persistence_id).await;

        self.storage
            .delete_all(persistence_id)
            .await
            .expect("delete all");

        let latest_snapshot = latest_snapshot.expect("load latest snapshot").unwrap();
        assert_eq!(latest_snapshot.sequence, 3);
    }

    pub async fn test_messages_read_write(&self) {
        let persistence_id = "hi";
        let entries = generate_entries(2);
        for entry in entries {
            self.storage
                .write_message(persistence_id, entry)
                .await
                .expect("write message");
        }

        let latest_messages = self.storage.read_latest_messages(persistence_id, 0).await;

        self.storage
            .delete_all(persistence_id)
            .await
            .expect("delete all");

        let latest_messages = latest_messages.unwrap().unwrap();
        assert_eq!(latest_messages.len(), 2);
        assert_eq!(latest_messages[0].sequence, 1);
        assert_eq!(latest_messages[1].sequence, 2);
    }

    pub async fn test_message_batch_read_write(&self) {
        let persistence_id = "hi";

        let entries = generate_entries(5);
        self.storage
            .write_message_batch(persistence_id, entries)
            .await
            .expect("write message batch");

        let latest_messages = self.storage.read_latest_messages(persistence_id, 0).await;

        self.storage
            .delete_all(persistence_id)
            .await
            .expect("delete all");

        let latest_messages = latest_messages.unwrap().unwrap();
        assert_eq!(latest_messages.len(), 5);
        assert_eq!(latest_messages[0].sequence, 1);
        assert_eq!(latest_messages[1].sequence, 2);
        assert_eq!(latest_messages[2].sequence, 3);
        assert_eq!(latest_messages[3].sequence, 4);
        assert_eq!(latest_messages[4].sequence, 5);
    }

    pub async fn test_single_message_read(&self) {
        let persistence_id = "hi";
        let entries = generate_entries(5);
        self.storage
            .write_message_batch(persistence_id, entries)
            .await
            .expect("write message batch");

        let third_message = self.storage.read_message(persistence_id, 3).await;

        self.storage
            .delete_all(persistence_id)
            .await
            .expect("delete all");

        let third_message = third_message.unwrap().unwrap();
        assert_eq!(third_message.sequence, 3);
    }

    pub async fn test_message_batch_delete_range(&self) {
        let persistence_id = "hi";
        let entries = generate_entries(5);
        self.storage
            .write_message_batch(persistence_id, entries)
            .await
            .expect("write message batch");

        self.storage
            .delete_messages_to(persistence_id, 3)
            .await
            .expect("delete message batch");

        let latest_messages = self.storage.read_latest_messages(persistence_id, 0).await;

        self.storage
            .delete_all(persistence_id)
            .await
            .expect("delete all");

        let latest_messages = latest_messages.unwrap().unwrap();
        assert_eq!(latest_messages.len(), 2);
    }
}

pub fn generate_entries(n: i32) -> Vec<JournalEntry> {
    (1..n + 1)
        .into_iter()
        .map(|n| JournalEntry {
            sequence: n as i64,
            payload_type: "test".into(),
            bytes: vec![1, 3, 3, 7].into(),
        })
        .collect()
}
