#[macro_use]
extern crate async_trait;

#[macro_use]
extern crate redis_async;
extern crate uuid;

use coerce_rt::actor::context::{ActorContext, ActorHandlerContext};
use coerce_rt::actor::message::{Handler, Message};
use coerce_rt::actor::{Actor, ActorRefErr};
use coerce_rt::worker::{Worker, WorkerRef, WorkerRefExt};
use redis_async::client::PairedConnection;
use redis_async::resp::{FromResp, RespValue};
use redis_async::{client, error::Error};
use std::marker::PhantomData;
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

#[async_trait]
pub trait RedisWorkerRefExt {
    async fn command<T: FromResp>(&mut self, cmd: RespValue) -> Result<T, RedisWorkerErr>
    where
        T: 'static + Sync + Send;
}

#[async_trait]
impl RedisWorkerRefExt for WorkerRef<RedisWorker> {
    async fn command<T: FromResp>(&mut self, cmd: RespValue) -> Result<T, RedisWorkerErr>
    where
        T: 'static + Sync + Send,
    {
        self.dispatch(RedisCommand::new(cmd)).await?
    }
}

pub struct RedisCommand<T: FromResp> {
    cmd: RespValue,
    _t: PhantomData<T>,
}

impl<T> Message for RedisCommand<T>
where
    T: FromResp,
{
    type Result = Result<T, RedisWorkerErr>;
}

impl<T: FromResp> RedisCommand<T> {
    pub fn new(cmd: RespValue) -> RedisCommand<T> {
        let _t = PhantomData;

        RedisCommand { cmd, _t }
    }
}

#[async_trait]
impl<T: FromResp> Handler<RedisCommand<T>> for RedisWorker
where
    T: 'static + Sync + Send,
{
    async fn handle(
        &mut self,
        message: RedisCommand<T>,
        _ctx: &mut ActorHandlerContext,
    ) -> Result<T, RedisWorkerErr> {
        Ok(self.client.send(message.cmd).await?)
    }
}

#[derive(Debug)]
pub enum RedisWorkerErr {
    Actor(ActorRefErr),
    Redis(Error),
    InvalidAddr,
}

impl From<AddrParseError> for RedisWorkerErr {
    fn from(_: AddrParseError) -> Self {
        RedisWorkerErr::InvalidAddr
    }
}

impl From<Error> for RedisWorkerErr {
    fn from(e: Error) -> Self {
        RedisWorkerErr::Redis(e)
    }
}

impl From<ActorRefErr> for RedisWorkerErr {
    fn from(e: ActorRefErr) -> Self {
        RedisWorkerErr::Actor(e)
    }
}
