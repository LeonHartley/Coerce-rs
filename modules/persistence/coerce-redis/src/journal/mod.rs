use crate::journal::actor::{
    Delete, DeleteRange, ReadMessage, ReadMessages, ReadSnapshot, RedisJournal, Write, WriteBatch,
};

use coerce::actor::system::ActorSystem;
use coerce::actor::{IntoActor, LocalActorRef};
use coerce::persistent::journal::provider::StorageProvider;
use coerce::persistent::journal::storage::{JournalEntry, JournalStorage, JournalStorageRef};

use redis::aio::ConnectionLike;

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use tokio::sync::oneshot;

pub(crate) mod actor;

#[derive(Clone)]
pub struct RedisStorageProvider {
    redis: JournalStorageRef,
}

pub struct RedisStorageConfig {
    pub nodes: Vec<String>,
    pub key_prefix: String,
    pub cluster: bool,
    pub use_key_hashtags: bool,
}

pub struct RedisJournalStorage<C: 'static + ConnectionLike + Send + Sync>
where
    C: Clone,
{
    redis_journal: LocalActorRef<RedisJournal<C>>,
    config: Arc<RedisStorageConfig>,
    key_provider_fn: fn(&str, &str, &RedisStorageConfig) -> String,
}

impl RedisStorageProvider {
    pub async fn connect(config: RedisStorageConfig, system: &ActorSystem) -> Self {
        // #[cfg(feature = "cluster")]
        // if config.cluster {
        //     return Self::clustered(config, system).await;
        // }

        Self::single_node(config, system).await
    }

    // #[cfg(feature = "cluster")]
    // pub async fn clustered(
    //     config: RedisStorageConfig,
    //     system: &ActorSystem,
    // ) -> RedisStorageProvider {
    //     use redis::cluster::ClusterClient;
    //
    //     create_provider(
    //         {
    //             let client = ClusterClient::new(config.nodes.clone()).unwrap();
    //             client.get_connection().unwrap()
    //         },
    //         config,
    //         system,
    //     )
    //     .await
    // }

    pub async fn single_node(
        config: RedisStorageConfig,
        system: &ActorSystem,
    ) -> RedisStorageProvider {
        use redis::Client;

        create_provider(
            {
                let client = Client::open(config.nodes[0].clone()).expect("redis client create");
                client.get_multiplexed_tokio_connection().await.unwrap()
            },
            config,
            system,
        )
        .await
    }
}

async fn create_provider<C: 'static + ConnectionLike + Send + Sync>(
    redis: C,
    config: RedisStorageConfig,
    system: &ActorSystem,
) -> RedisStorageProvider
where
    C: Clone,
{
    const REDIS_JOURNAL_COUNTER: AtomicU32 = AtomicU32::new(1);
    let config = Arc::new(config);

    let redis_journal = RedisJournal(redis)
        .into_actor(
            Some(format!(
                "redis-journal-{}",
                REDIS_JOURNAL_COUNTER.fetch_add(1, Ordering::Relaxed)
            )),
            system,
        )
        .await
        .expect("start journal actor");

    let redis = Arc::new(RedisJournalStorage {
        redis_journal,
        config: config.clone(),
        key_provider_fn: if config.use_key_hashtags {
            get_clustered_redis_key
        } else {
            get_redis_key
        },
    });

    RedisStorageProvider { redis }
}

impl StorageProvider for RedisStorageProvider {
    fn journal_storage(&self) -> Option<JournalStorageRef> {
        Some(self.redis.clone())
    }
}

#[async_trait]
impl<C: ConnectionLike + Send + Sync> JournalStorage for RedisJournalStorage<C>
where
    C: Clone,
{
    async fn write_snapshot(
        &self,
        persistence_id: &str,
        entry: JournalEntry,
    ) -> anyhow::Result<()> {
        let (tx, rx) = oneshot::channel();
        let key = (self.key_provider_fn)(persistence_id, "snapshot", self.config.as_ref());

        self.redis_journal.notify(Write {
            key,
            entry,
            result_channel: tx,
        })?;

        rx.await?
    }

    async fn write_message(&self, persistence_id: &str, entry: JournalEntry) -> anyhow::Result<()> {
        let (result_channel, rx) = oneshot::channel();
        let key = (self.key_provider_fn)(persistence_id, "journal", self.config.as_ref());

        self.redis_journal.notify(Write {
            key,
            entry,
            result_channel,
        })?;

        rx.await?
    }

    async fn write_message_batch(
        &self,
        persistence_id: &str,
        entries: Vec<JournalEntry>,
    ) -> anyhow::Result<()> {
        let (result_channel, rx) = oneshot::channel();
        let key = (self.key_provider_fn)(persistence_id, "journal", self.config.as_ref());

        self.redis_journal.notify(WriteBatch {
            key,
            entries,
            result_channel,
        })?;

        rx.await?
    }

    async fn read_latest_snapshot(
        &self,
        persistence_id: &str,
    ) -> anyhow::Result<Option<JournalEntry>> {
        let (result_channel, rx) = oneshot::channel();
        let key = (self.key_provider_fn)(persistence_id, "snapshot", self.config.as_ref());

        self.redis_journal
            .notify(ReadSnapshot(key, result_channel))?;

        rx.await?
    }

    async fn read_latest_messages(
        &self,
        persistence_id: &str,
        from_sequence: i64,
    ) -> anyhow::Result<Option<Vec<JournalEntry>>> {
        let (result_channel, rx) = oneshot::channel();
        let key = (self.key_provider_fn)(persistence_id, "journal", self.config.as_ref());
        self.redis_journal.notify(ReadMessages {
            key,
            start_sequence: Some(from_sequence),
            end_sequence: None,
            result_channel,
        })?;

        rx.await?
    }

    async fn read_message(
        &self,
        persistence_id: &str,
        sequence_id: i64,
    ) -> anyhow::Result<Option<JournalEntry>> {
        let (result_channel, rx) = oneshot::channel();
        let key = (self.key_provider_fn)(persistence_id, "journal", self.config.as_ref());

        self.redis_journal.notify(ReadMessage {
            key,
            sequence_id,
            result_channel,
        })?;

        rx.await?
    }

    async fn read_messages(
        &self,
        persistence_id: &str,
        from_sequence: i64,
        to_sequence: i64,
    ) -> anyhow::Result<Option<Vec<JournalEntry>>> {
        let (result_channel, rx) = oneshot::channel();
        let key = (self.key_provider_fn)(persistence_id, "journal", self.config.as_ref());
        self.redis_journal.notify(ReadMessages {
            key,
            start_sequence: Some(from_sequence),
            end_sequence: Some(to_sequence),
            result_channel,
        })?;

        rx.await?
    }

    async fn delete_messages_to(
        &self,
        persistence_id: &str,
        to_sequence: i64,
    ) -> anyhow::Result<()> {
        let (result_channel, rx) = oneshot::channel();
        let key = (self.key_provider_fn)(persistence_id, "journal", self.config.as_ref());
        self.redis_journal.notify(DeleteRange {
            key,
            start_sequence: 0,
            end_sequence: to_sequence,
            result_channel,
        })?;

        rx.await?
    }

    async fn delete_all(&self, persistence_id: &str) -> anyhow::Result<()> {
        let journal_key = (self.key_provider_fn)(persistence_id, "journal", self.config.as_ref());
        let snapshot_key = (self.key_provider_fn)(persistence_id, "snapshot", self.config.as_ref());

        self.redis_journal
            .send(Delete(vec![journal_key, snapshot_key]))
            .await?
    }
}

fn get_clustered_redis_key(
    persistence_id: &str,
    value_type: &str,
    config: &RedisStorageConfig,
) -> String {
    format!(
        "{{__{persistence_id}}}:{key_prefix}{persistence_id}:{value_type}",
        persistence_id = persistence_id,
        key_prefix = config.key_prefix,
        value_type = value_type
    )
}

fn get_redis_key(persistence_id: &str, value_type: &str, config: &RedisStorageConfig) -> String {
    format!(
        "{key_prefix}{persistence_id}:{value_type}",
        persistence_id = persistence_id,
        key_prefix = config.key_prefix,
        value_type = value_type
    )
}
