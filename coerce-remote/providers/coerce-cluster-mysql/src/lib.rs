use coerce_remote::cluster::workers::{WorkerStore, WorkerStoreErr, ClusterWorker};

#[macro_use]
extern crate async_trait;

pub struct MySqlWorkerStore {

}

#[async_trait]
impl WorkerStore for MySqlWorkerStore {
    async fn get_active(&mut self) -> Result<Vec<ClusterWorker>, WorkerStoreErr> {
        unimplemented!()
    }

    async fn put(&mut self, worker: &ClusterWorker) -> Result<(), WorkerStoreErr> {
        unimplemented!()
    }

    async fn remove(&mut self, worker: &ClusterWorker) -> Result<(), WorkerStoreErr> {
        unimplemented!()
    }
}

