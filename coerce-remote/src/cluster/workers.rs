use crate::cluster::node::RemoteNode;
use chrono::{DateTime, Utc};
use coerce_rt::actor::context::{ActorContext, ActorHandlerContext};
use coerce_rt::actor::message::{Handler, Message};
use coerce_rt::actor::{Actor, ActorRef, ActorRefErr};
use std::error::Error;
use std::time::Instant;
use uuid::Uuid;

#[derive(Debug)]
pub enum WorkerStoreErr {
    Actor(ActorRefErr),
    Database(String),
}

#[async_trait]
pub trait WorkerStore {
    async fn get_active(&mut self) -> Result<Vec<ClusterWorker>, WorkerStoreErr>;

    async fn put(&mut self, worker: &ClusterWorker) -> Result<(), WorkerStoreErr>;

    async fn remove(&mut self, worker: &ClusterWorker) -> Result<(), WorkerStoreErr>;
}

pub struct ClusterWorkers {
    store: Box<dyn WorkerStore + Send + Sync>,
}

impl ClusterWorkers {
    pub async fn new<T: WorkerStore>(
        store: T,
        context: &mut ActorContext,
    ) -> Result<ActorRef<ClusterWorkers>, WorkerStoreErr>
    where
        T: 'static + Sync + Send,
    {
        let store = Box::new(store);
        Ok(context.new_anon_actor(ClusterWorkers { store }).await?)
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
        self.store.put(&message.0).await
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

impl From<ActorRefErr> for WorkerStoreErr {
    fn from(e: ActorRefErr) -> Self {
        WorkerStoreErr::Actor(e)
    }
}
