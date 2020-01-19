use crate::{RedisWorkerRef};
use coerce_remote::cluster::workers::{ClusterWorker, WorkerStore, WorkerStoreErr};

pub struct RedisWorkerStore {}

impl RedisWorkerStore {
    pub fn new(_redis: &RedisWorkerRef) -> RedisWorkerStore {
        RedisWorkerStore {}
    }
}

#[async_trait]
impl WorkerStore for RedisWorkerStore {
    async fn get_active(&mut self) -> Result<Vec<ClusterWorker>, WorkerStoreErr> {
        unimplemented!()
    }

    async fn put(&mut self, _worker: &ClusterWorker) -> Result<(), WorkerStoreErr> {
        unimplemented!()
    }

    async fn remove(&mut self, _worker: &ClusterWorker) -> Result<(), WorkerStoreErr> {
        unimplemented!()
    }
}
