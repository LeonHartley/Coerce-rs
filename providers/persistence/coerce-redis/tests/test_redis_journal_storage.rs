use coerce::actor::system::ActorSystem;
use coerce::persistent::journal::provider::StorageProvider;
use coerce::persistent::journal::storage::JournalEntry;
use coerce::persistent::storage::JournalStorageRef;
use coerce::persistent::Persistence;

use coerce_redis::journal::{RedisStorageConfig, RedisStorageProvider};

const TEST_REDIS_HOST: &str = "redis://127.0.0.1:6379/";

struct RedisTestCtx {
    system: ActorSystem,
    storage: JournalStorageRef,
}

#[tokio::test]
pub async fn test_redis_journal_read_write_snapshot() {
    let persistence_id = "hi";
    let ctx = new_test_context("test_redis_journal_read_write_snapshot:").await;
    let redis = ctx.storage;
    let entries = generate_entries(3);

    for entry in entries {
        redis
            .write_snapshot(persistence_id, entry)
            .await
            .expect("write snapshot");
    }

    let latest_snapshot = redis.read_latest_snapshot(persistence_id).await;

    redis.delete_all(persistence_id).await.expect("delete all");

    let latest_snapshot = latest_snapshot.expect("load latest snapshot").unwrap();
    assert_eq!(latest_snapshot.sequence, 3);
}

#[tokio::test]
pub async fn test_redis_journal_read_write_messages() {
    let persistence_id = "hi";
    let ctx = new_test_context("test_redis_journal_read_write_messages:").await;
    let redis = ctx.storage;

    let entries = generate_entries(2);
    for entry in entries {
        redis
            .write_message(persistence_id, entry)
            .await
            .expect("write message");
    }

    let latest_messages = redis.read_latest_messages(persistence_id, 0).await;

    redis.delete_all(persistence_id).await.expect("delete all");

    let latest_messages = latest_messages.unwrap().unwrap();
    assert_eq!(latest_messages.len(), 2);
    assert_eq!(latest_messages[0].sequence, 1);
    assert_eq!(latest_messages[1].sequence, 2);
}

#[tokio::test]
pub async fn test_redis_journal_read_write_message_batch() {
    let persistence_id = "hi";
    let ctx = new_test_context("test_redis_journal_read_write_message_batch:").await;
    let redis = ctx.storage;

    let entries = generate_entries(5);
    redis
        .write_message_batch(persistence_id, entries)
        .await
        .expect("write message batch");

    let latest_messages = redis.read_latest_messages(persistence_id, 0).await;

    redis.delete_all(persistence_id).await.expect("delete all");

    let latest_messages = latest_messages.unwrap().unwrap();
    assert_eq!(latest_messages.len(), 5);
    assert_eq!(latest_messages[0].sequence, 1);
    assert_eq!(latest_messages[1].sequence, 2);
    assert_eq!(latest_messages[2].sequence, 3);
    assert_eq!(latest_messages[3].sequence, 4);
    assert_eq!(latest_messages[4].sequence, 5);
}

#[tokio::test]
pub async fn test_redis_journal_read_single_message() {
    let persistence_id = "hi";
    let ctx = new_test_context("test_redis_journal_read_single_message:").await;
    let redis = ctx.storage;

    let entries = generate_entries(5);
    redis
        .write_message_batch(persistence_id, entries)
        .await
        .expect("write message batch");

    let third_message = redis.read_message(persistence_id, 3).await;

    redis.delete_all(persistence_id).await.expect("delete all");

    let third_message = third_message.unwrap().unwrap();
    assert_eq!(third_message.sequence, 3);
}

#[tokio::test]
pub async fn test_redis_journal_delete_messages_range() {
    let persistence_id = "hi";
    let ctx = new_test_context("test_redis_journal_delete_messages_range:").await;
    let redis = ctx.storage;

    let entries = generate_entries(5);
    redis
        .write_message_batch(persistence_id, entries)
        .await
        .expect("write message batch");

    redis
        .delete_messages_to(persistence_id, 3)
        .await
        .expect("delete message batch");

    let latest_messages = redis.read_latest_messages(persistence_id, 0).await;

    redis.delete_all(persistence_id).await.expect("delete all");

    let latest_messages = latest_messages.unwrap().unwrap();
    assert_eq!(latest_messages.len(), 2);
}

async fn new_test_context(key_prefix: &str) -> RedisTestCtx {
    let system = ActorSystem::new();
    let provider = RedisStorageProvider::connect(
        RedisStorageConfig {
            nodes: vec![TEST_REDIS_HOST.to_string()],
            key_prefix: key_prefix.to_string(),
            cluster: false,
            use_key_hashtags: false,
        },
        &system,
    )
    .await;

    let storage = provider.journal_storage().expect("journal storage");
    system.to_persistent(provider.into());
    RedisTestCtx { system, storage }
}

fn generate_entries(n: i32) -> Vec<JournalEntry> {
    (1..n + 1)
        .into_iter()
        .map(|n| JournalEntry {
            sequence: n as i64,
            payload_type: "test".into(),
            bytes: vec![1, 3, 3, 7].into(),
        })
        .collect()
}
