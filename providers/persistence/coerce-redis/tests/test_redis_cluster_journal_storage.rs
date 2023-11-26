#[cfg(feature = "cluster")]

mod util;

use crate::util::RedisTest;
use coerce::persistent::journal::provider::StorageProvider;

#[tokio::test]
pub async fn test_redis_cluster_journal_read_write_snapshot() {
    RedisTest::new(true, "test_redis_cluster_journal_read_write_snapshot:")
        .await
        .test_snapshot_read_write()
        .await
}

#[tokio::test]
pub async fn test_redis_cluster_journal_read_write_messages() {
    RedisTest::new(true, "test_redis_cluster_journal_read_write_messages:")
        .await
        .test_messages_read_write()
        .await
}

#[tokio::test]
pub async fn test_redis_cluster_journal_read_write_message_batch() {
    RedisTest::new(true, "test_redis_cluster_journal_read_write_message_batch:")
        .await
        .test_message_batch_read_write()
        .await
}

#[tokio::test]
pub async fn test_redis_cluster_journal_read_single_message() {
    RedisTest::new(true, "test_redis_cluster_journal_read_single_message:")
        .await
        .test_message_batch_read_write()
        .await
}

#[tokio::test]
pub async fn test_redis_cluster_journal_delete_messages_range() {
    RedisTest::new(true, "test_redis_cluster_journal_delete_messages_range:")
        .await
        .test_message_batch_read_write()
        .await
}
