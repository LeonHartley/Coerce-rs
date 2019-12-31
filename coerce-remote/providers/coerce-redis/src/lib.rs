#[macro_use]
extern crate async_trait;

use coerce_rt::actor::context::ActorContext;
use coerce_rt::actor::{Actor, ActorRefErr};
use coerce_rt::worker::{Worker, WorkerRef};
use redis_async::client::PairedConnection;
use redis_async::{client, error::Error};
use std::net::AddrParseError;

pub mod actors;
pub mod workers;

pub type RedisWorkerRef = WorkerRef<RedisWorker>;

#[derive(Clone)]
pub struct RedisWorker {
    client: PairedConnection,
}

impl Actor for RedisWorker {}

impl RedisWorker {
    pub async fn new(
        addr: String,
        workers: usize,
        context: &mut ActorContext,
    ) -> Result<RedisWorkerRef, RedisWorkerErr> {
        let client = client::paired_connect(&addr.parse()?).await?;
        let worker = Worker::new(RedisWorker { client }, workers, context).await?;

        Ok(worker)
    }
}

pub enum RedisWorkerErr {
    Actor(ActorRefErr),
    Connection(Error),
    InvalidAddr,
}

impl From<AddrParseError> for RedisWorkerErr {
    fn from(_: AddrParseError) -> Self {
        RedisWorkerErr::InvalidAddr
    }
}

impl From<Error> for RedisWorkerErr {
    fn from(e: Error) -> Self {
        RedisWorkerErr::Connection(e)
    }
}

impl From<ActorRefErr> for RedisWorkerErr {
    fn from(e: ActorRefErr) -> Self {
        RedisWorkerErr::Actor(e)
    }
}
