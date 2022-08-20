use coerce::persistent::journal::provider::StorageProvider;
use coerce::persistent::journal::storage::{JournalEntry, JournalStorage, JournalStorageRef};
use coerce::remote::net::StreamData;
use redis_async::client;
use redis_async::client::PairedConnection;
use redis_async::resp::RespValue;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;

pub struct RedisStorageProvider {
    redis: PairedConnection,
    config: Arc<RedisStorageConfig>,
}

struct Redis {
    config: RedisStorageConfig,
}

pub struct RedisStorageConfig {
    pub address: String,
    pub key_prefix: String,
    pub use_key_hashtags: bool,
}

pub struct RedisJournalStorage {
    redis: PairedConnection,
    config: Arc<RedisStorageConfig>,
}

impl RedisStorageProvider {
    pub async fn connect(config: RedisStorageConfig) -> RedisStorageProvider {
        let config = Arc::new(config);
        let address = SocketAddr::from_str(&config.address).expect("invalid redis address");
        let redis = client::paired_connect(address.to_string())
            .await
            .expect("redis connect");

        RedisStorageProvider { redis, config }
    }
}

#[async_trait]
impl StorageProvider for RedisStorageProvider {
    fn journal_storage(&self) -> Option<JournalStorageRef> {
        Some(Arc::new(RedisJournalStorage {
            redis: self.redis.clone(),
            config: self.config.clone(),
        }))
    }
}

#[async_trait]
impl JournalStorage for RedisJournalStorage {
    async fn write_snapshot(
        &self,
        persistence_id: &str,
        entry: JournalEntry,
    ) -> anyhow::Result<()> {
        let key = [&self.config.key_prefix, persistence_id, ".snapshot"].concat();
        let command = resp_array![
            "ZADD",
            key,
            &entry.sequence.to_string(),
            entry.write_to_bytes().expect("serialized journal")
        ];

        let _ = self.redis.send::<Option<i32>>(command).await?;
        Ok(())
    }

    async fn write_message(&self, persistence_id: &str, entry: JournalEntry) -> anyhow::Result<()> {
        let key = [&self.config.key_prefix, persistence_id, ".journal"].concat();
        let command = resp_array![
            "ZADD",
            key,
            &entry.sequence.to_string(),
            entry.write_to_bytes().expect("serialized msg")
        ];

        let _ = self.redis.send::<Option<i32>>(command).await?;
        Ok(())
    }

    async fn read_latest_snapshot(
        &self,
        persistence_id: &str,
    ) -> anyhow::Result<Option<JournalEntry>> {
        let key = [&self.config.key_prefix, persistence_id, ".snapshot"].concat();
        let command = resp_array!["ZREVRANGEBYSCORE", key, "+inf", "-inf", "LIMIT", "0", "1"];
        let data = self.redis.send::<Option<RespValue>>(command).await?;

        Ok(match data {
            Some(data) => read_journal_entry(data),
            None => None,
        })
    }

    async fn read_latest_messages(
        &self,
        persistence_id: &str,
        from_sequence: i64,
    ) -> anyhow::Result<Option<Vec<JournalEntry>>> {
        let key = [&self.config.key_prefix, persistence_id, ".journal"].concat();
        let command = resp_array!["ZRANGEBYSCORE", key, from_sequence.to_string(), "+inf"];
        let data = self.redis.send::<Option<RespValue>>(command).await?;

        Ok(match data {
            Some(data) => read_journal_entry_array(data),
            None => None,
        })
    }

    async fn delete_all(&self, persistence_id: &str) -> anyhow::Result<()> {
        let journal_key = [&self.config.key_prefix, persistence_id, ".journal"].concat();
        let snapshot_key = [&self.config.key_prefix, persistence_id, ".snapshot"].concat();

        let command = resp_array!["DEL", journal_key, snapshot_key];
        let _ = self.redis.send::<Option<i32>>(command).await?;

        Ok(())
    }
}

fn read_journal_entry_array(redis_value: RespValue) -> Option<Vec<JournalEntry>> {
    match redis_value {
        RespValue::Array(array) => Some(
            array
                .into_iter()
                .map(|v| match v {
                    RespValue::BulkString(data) => JournalEntry::read_from_bytes(data).unwrap(),
                    _ => panic!("not supported"),
                })
                .collect(),
        ),
        _ => panic!("not supported"),
    }
}

fn read_journal_entry(redis_value: RespValue) -> Option<JournalEntry> {
    match redis_value {
        RespValue::Array(array) => {
            for data in array {
                return match data {
                    RespValue::BulkString(data) => {
                        Some(JournalEntry::read_from_bytes(data).unwrap())
                    }
                    _ => panic!("not supported"),
                };
            }

            None
        }
        _ => panic!("not supported"),
    }
}
