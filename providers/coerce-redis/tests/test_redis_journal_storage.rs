use coerce::persistent::journal::provider::StorageProvider;
use coerce::persistent::journal::storage::JournalEntry;
use coerce::persistent::journal::types::JournalTypes;
use coerce::persistent::PersistentActor;
use coerce_redis::journal::{RedisStorageConfig, RedisStorageProvider};

#[tokio::test]
pub async fn test_redis_journal_read_write_snapshot() {
    let persistence_id = "hi";
    let provider = RedisStorageProvider::connect(RedisStorageConfig {
        address: "127.0.0.1:6379".to_string(),
        key_prefix: "test_redis_journal_read_write_snapshot-".to_string(),
        use_key_hashtags: true,
    })
    .await;

    let redis = provider.journal_storage().expect("redis journal storage");

    redis
        .write_snapshot(
            persistence_id,
            JournalEntry {
                sequence: 1,
                payload_type: "test".to_string(),
                bytes: vec![1, 3, 3, 7],
            },
        )
        .await
        .expect("write snapshot");

    redis
        .write_snapshot(
            persistence_id,
            JournalEntry {
                sequence: 2,
                payload_type: "test".to_string(),
                bytes: vec![1, 3, 3, 7],
            },
        )
        .await
        .expect("write snapshot");

    redis
        .write_snapshot(
            persistence_id,
            JournalEntry {
                sequence: 3,
                payload_type: "test".to_string(),
                bytes: vec![1, 3, 3, 7],
            },
        )
        .await
        .expect("write snapshot");

    let latest_snapshot = redis.read_latest_snapshot(persistence_id).await;

    redis.delete_all(persistence_id).await;

    let latest_snapshot = latest_snapshot.expect("load latest snapshot").unwrap();

    assert_eq!(latest_snapshot.sequence, 3);
}

#[tokio::test]
pub async fn test_redis_journal_read_write_messages() {
    let persistence_id = "hi";

    let provider = RedisStorageProvider::connect(RedisStorageConfig {
        address: "127.0.0.1:6379".to_string(),
        key_prefix: "test_redis_journal_read_write_messages-".to_string(),
        use_key_hashtags: true,
    })
    .await;

    let redis = provider.journal_storage().expect("redis journal storage");

    redis
        .write_message(
            persistence_id,
            JournalEntry {
                sequence: 1,
                payload_type: "test".to_string(),
                bytes: vec![1, 3, 3, 7],
            },
        )
        .await;

    redis
        .write_message(
            persistence_id,
            JournalEntry {
                sequence: 2,
                payload_type: "test".to_string(),
                bytes: vec![1, 3, 3, 7],
            },
        )
        .await;

    let latest_messages = redis.read_latest_messages(persistence_id, 0).await;

    redis.delete_all(persistence_id).await;

    let latest_messages = latest_messages.unwrap().unwrap();
    assert_eq!(latest_messages.len(), 2);
    assert_eq!(latest_messages[0].sequence, 1);
    assert_eq!(latest_messages[1].sequence, 2);
}

#[tokio::test]
pub async fn test_redis_journal_actor_integration() {}
