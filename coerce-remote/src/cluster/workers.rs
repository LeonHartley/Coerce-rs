use crate::cluster::node::RemoteNode;
use chrono::{DateTime, Utc};
use std::error::Error;
use std::time::Instant;
use uuid::Uuid;

pub enum WorkerStoreErr {}

#[async_trait]
pub trait WorkerStore {
    async fn get_active() -> Result<Vec<ClusterWorker>, WorkerStoreErr>;

    async fn update(worker: &ClusterWorker) -> Result<(), WorkerStoreErr>;
}

pub struct ClusterWorker {
    id: Uuid,
    node: RemoteNode,
    last_ping: DateTime<Utc>,
}
