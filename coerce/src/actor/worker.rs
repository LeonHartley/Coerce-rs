use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message};
use crate::actor::scheduler::ActorType::Anonymous;
use crate::actor::system::ActorSystem;
use crate::actor::{Actor, ActorRefErr, LocalActorRef};
use std::collections::VecDeque;

pub type WorkerRef<W> = LocalActorRef<Worker<W>>;

pub struct Worker<W: Actor>
where
    W: 'static + Sync + Send,
{
    workers: VecDeque<LocalActorRef<W>>,
}

impl<W: Actor> Worker<W>
where
    W: 'static + Clone + Sync + Send,
{
    pub async fn new(
        state: W,
        count: usize,
        name_prefix: &'static str,
        system: &mut ActorSystem,
    ) -> Result<WorkerRef<W>, ActorRefErr> {
        let mut workers = VecDeque::with_capacity(count);

        for i in 0..count {
            workers.push_back(
                system
                    .new_actor(
                        format!("{}-{}", name_prefix, i + 1),
                        state.clone(),
                        Anonymous,
                    )
                    .await?,
            );
        }

        Ok(system
            .new_actor(
                format!("{}-supervisor", name_prefix),
                Worker { workers },
                Anonymous,
            )
            .await?)
    }
}

#[async_trait]
pub trait IntoWorker<W: Actor>
where
    W: 'static + Clone + Sync + Send,
{
    async fn into_worker(
        self,
        count: usize,
        name_prefix: &'static str,
        sys: &mut ActorSystem,
    ) -> Result<WorkerRef<W>, ActorRefErr>;
}

#[async_trait]
impl<W: Actor> IntoWorker<W> for W
where
    W: 'static + Clone + Sync + Send,
{
    async fn into_worker(
        self,
        count: usize,
        name_prefix: &'static str,
        sys: &mut ActorSystem,
    ) -> Result<WorkerRef<W>, ActorRefErr> {
        Worker::new(self, count, name_prefix, sys).await
    }
}

impl<W: Actor> Actor for Worker<W> where W: 'static + Clone + Sync + Send {}

#[async_trait]
pub trait WorkerRefExt<W: Actor> {
    async fn dispatch<M: Message>(&mut self, message: M) -> Result<M::Result, ActorRefErr>
    where
        M: 'static + Send + Sync,
        M::Result: 'static + Send + Sync,
        W: Handler<M>;
}

pub struct WorkerMessage<M: Message>
where
    M: 'static + Sync + Send,
{
    message: M,
    res_tx: tokio::sync::oneshot::Sender<M::Result>,
}

impl<M: Message> WorkerMessage<M>
where
    M: 'static + Sync + Send,
    M::Result: 'static + Sync + Send,
{
    pub fn new(message: M, res_tx: tokio::sync::oneshot::Sender<M::Result>) -> WorkerMessage<M> {
        WorkerMessage { message, res_tx }
    }
}

impl<M: Message> Message for WorkerMessage<M>
where
    M: 'static + Sync + Send,
    M::Result: 'static + Sync + Send,
{
    type Result = ();
}

#[async_trait]
impl<W: Actor> WorkerRefExt<W> for LocalActorRef<Worker<W>>
where
    W: 'static + Clone + Sync + Send,
{
    async fn dispatch<M: Message>(&mut self, message: M) -> Result<M::Result, ActorRefErr>
    where
        M: 'static + Send + Sync,
        M::Result: 'static + Send + Sync,
        W: Handler<M>,
    {
        let (res_tx, res) = tokio::sync::oneshot::channel();
        let message = WorkerMessage::new(message, res_tx);

        if let Err(e) = self.send(message).await {
            Err(e)
        } else {
            match res.await {
                Ok(res) => Ok(res),
                Err(_e) => {
                    error!(target: "WorkerRef", "error receiving result");
                    Err(ActorRefErr::ActorUnavailable)
                }
            }
        }
    }
}

#[async_trait]
impl<W: Actor, M: Message> Handler<WorkerMessage<M>> for Worker<W>
where
    W: 'static + Sync + Send,
    M: 'static + Sync + Send,
    W: Handler<M>,
    M::Result: 'static + Sync + Send,
{
    async fn handle(&mut self, message: WorkerMessage<M>, _ctx: &mut ActorContext) {
        if let Some(worker) = self.workers.pop_front() {
            let mut worker_ref = worker.clone();

            self.workers.push_back(worker);

            // main worker acts as a scheduler, don't block it by handling the task, dispatch it off
            tokio::spawn(async move {
                match worker_ref.send(message.message).await {
                    Ok(res) => {
                        if message.res_tx.send(res).is_ok() {
                            trace!(target: "Worker", "sent result successfully");
                        } else {
                            error!(target: "Worker", "failed to send result, receiver dropped?");
                        }
                    }
                    Err(_e) => error!(target: "Worker", "error sending msg"),
                }
            });
        }
    }
}
