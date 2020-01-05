use crate::{RedisWorkerErr, RedisWorkerRef, RedisWorkerRefExt};
use coerce_remote::cluster::workers::{ClusterWorker, WorkerStore, WorkerStoreErr};

pub struct RedisWorkerStore {
    redis: RedisWorkerRef,
}

impl RedisWorkerStore {
    pub fn new(redis: &RedisWorkerRef) -> RedisWorkerStore {
        let redis = redis.clone();
        RedisWorkerStore { redis }
    }
}

impl WorkerStore for RedisWorkerStore {
    async fn get_active(&mut self) -> Result<Vec<ClusterWorker>, WorkerStoreErr> {
        self.redis
            .command(resp_array!["SET", key, actor.state.as_slice()])
            .await?;

        Ok(vec![])
    }

    async fn put(&mut self, worker: &ClusterWorker) -> Result<(), WorkerStoreErr> {
        unimplemented!()
    }

    async fn remove(&mut self, worker: &ClusterWorker) -> Result<(), WorkerStoreErr> {
        unimplemented!()
    }
}
