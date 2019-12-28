use crate::cluster::node::RemoteNode;
use chrono::{DateTime, Utc};
use coerce_rt::actor::context::ActorHandlerContext;
use coerce_rt::actor::message::{Handler, Message};
use coerce_rt::actor::Actor;
use std::error::Error;
use std::time::Instant;
use uuid::Uuid;

#[derive(Debug)]
pub enum WorkerStoreErr {}

#[async_trait]
pub trait WorkerStore {
    async fn get_active(&mut self) -> Result<Vec<ClusterWorker>, WorkerStoreErr>;

    async fn update(&mut self, worker: &ClusterWorker) -> Result<(), WorkerStoreErr>;
}

pub struct ClusterWorkers {
    store: Box<dyn WorkerStore + Send + Sync>,
}

impl ClusterWorkers {
    pub fn new<T: WorkerStore>(store: T) -> ClusterWorkers
    where
        T: 'static + Sync + Send,
    {
        let store = Box::new(store);
        ClusterWorkers { store }
    }
}

impl Actor for ClusterWorkers {}

pub struct GetActiveWorkers;

impl Message for GetActiveWorkers {
    type Result = Result<Vec<ClusterWorker>, WorkerStoreErr>;
}

pub struct UpdateWorker(ClusterWorker);

impl Message for UpdateWorker {
    type Result = Result<(), WorkerStoreErr>;
}

#[async_trait]
impl Handler<GetActiveWorkers> for ClusterWorkers {
    async fn handle(
        &mut self,
        message: GetActiveWorkers,
        ctx: &mut ActorHandlerContext,
    ) -> Result<Vec<ClusterWorker>, WorkerStoreErr> {
        self.store.get_active().await
    }
}

#[async_trait]
impl Handler<UpdateWorker> for ClusterWorkers {
    async fn handle(
        &mut self,
        message: UpdateWorker,
        ctx: &mut ActorHandlerContext,
    ) -> Result<(), WorkerStoreErr> {
        self.store.update(&message.0).await
    }
}

pub struct ClusterWorker {
    id: Uuid,
    node: RemoteNode,
    last_ping: Option<DateTime<Utc>>,
}

impl ClusterWorker {
    pub fn new(id: Uuid, node: RemoteNode, last_ping: Option<DateTime<Utc>>) -> ClusterWorker {
        ClusterWorker {
            id,
            node,
            last_ping,
        }
    }
}
