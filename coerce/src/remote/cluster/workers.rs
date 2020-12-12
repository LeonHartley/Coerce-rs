use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message};
use crate::actor::{Actor, ActorRefErr, LocalActorRef};
use crate::remote::cluster::node::RemoteNode;
use chrono::{DateTime, Utc};

use crate::actor::system::ActorSystem;
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
        system: &mut ActorSystem,
    ) -> Result<LocalActorRef<ClusterWorkers>, WorkerStoreErr>
    where
        T: 'static + Sync + Send,
    {
        let store = Box::new(store);
        Ok(system.new_anon_actor(ClusterWorkers { store }).await?)
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
        _message: GetActiveWorkers,
        _ctx: &mut ActorContext,
    ) -> Result<Vec<ClusterWorker>, WorkerStoreErr> {
        self.store.get_active().await
    }
}

#[async_trait]
impl Handler<UpdateWorker> for ClusterWorkers {
    async fn handle(
        &mut self,
        message: UpdateWorker,
        _ctx: &mut ActorContext,
    ) -> Result<(), WorkerStoreErr> {
        self.store.put(&message.0).await
    }
}

pub struct ClusterWorker {}

impl ClusterWorker {
    pub fn new(_id: Uuid, _node: RemoteNode, _last_ping: Option<DateTime<Utc>>) -> ClusterWorker {
        ClusterWorker {}
    }
}

impl From<ActorRefErr> for WorkerStoreErr {
    fn from(e: ActorRefErr) -> Self {
        WorkerStoreErr::Actor(e)
    }
}
