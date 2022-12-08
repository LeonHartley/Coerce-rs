use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message};
use crate::actor::scheduler::ActorType::Anonymous;
use crate::actor::system::ActorSystem;
use crate::actor::{Actor, ActorRefErr, LocalActorRef};
use std::collections::VecDeque;

pub type WorkerRef<W> = LocalActorRef<Worker<W>>;

pub struct Worker<W: Actor> {
    workers: VecDeque<LocalActorRef<W>>,
}

impl<W: Actor + Clone> Worker<W> {
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

        system
            .new_actor(
                format!("{}-supervisor", name_prefix),
                Worker { workers },
                Anonymous,
            )
            .await
    }
}

#[async_trait]
pub trait IntoWorker<W: Actor + Clone> {
    async fn into_worker(
        self,
        count: usize,
        name_prefix: &'static str,
        sys: &mut ActorSystem,
    ) -> Result<WorkerRef<W>, ActorRefErr>;
}

#[async_trait]
impl<W: Actor + Clone> IntoWorker<W> for W {
    async fn into_worker(
        self,
        count: usize,
        name_prefix: &'static str,
        sys: &mut ActorSystem,
    ) -> Result<WorkerRef<W>, ActorRefErr> {
        Worker::new(self, count, name_prefix, sys).await
    }
}

impl<W: Actor + Clone> Actor for Worker<W> {}

#[async_trait]
pub trait WorkerRefExt<W: Actor + Clone> {
    async fn dispatch<M: Message>(&mut self, message: M) -> Result<M::Result, ActorRefErr>
    where
        W: Handler<M>;
}

pub struct WorkerMessage<M: Message> {
    message: M,
    res_tx: tokio::sync::oneshot::Sender<M::Result>,
}

impl<M: Message> WorkerMessage<M> {
    pub fn new(message: M, res_tx: tokio::sync::oneshot::Sender<M::Result>) -> WorkerMessage<M> {
        WorkerMessage { message, res_tx }
    }
}

impl<M: Message> Message for WorkerMessage<M> {
    type Result = ();
}

#[async_trait]
impl<W: Actor + Clone> WorkerRefExt<W> for LocalActorRef<Worker<W>> {
    async fn dispatch<M: Message>(&mut self, message: M) -> Result<M::Result, ActorRefErr>
    where
        W: Handler<M>,
    {
        let (res_tx, res) = tokio::sync::oneshot::channel();
        let message = WorkerMessage::new(message, res_tx);

        if let Err(e) = self.send(message).await {
            Err(e)
        } else {
            match res.await {
                Ok(res) => Ok(res),
                Err(e) => {
                    error!("error receiving result, {}", e);
                    Err(ActorRefErr::ResultChannelClosed)
                }
            }
        }
    }
}

#[async_trait]
impl<W: Actor + Clone, M: Message> Handler<WorkerMessage<M>> for Worker<W>
where
    W: Handler<M>,
{
    async fn handle(&mut self, message: WorkerMessage<M>, _ctx: &mut ActorContext) {
        if let Some(worker) = self.workers.pop_front() {
            let worker_ref = worker.clone();

            self.workers.push_back(worker);

            // main worker acts as a scheduler, don't block it by handling the task, dispatch it off
            tokio::spawn(async move {
                match worker_ref.send(message.message).await {
                    Ok(res) => {
                        if message.res_tx.send(res).is_ok() {
                            trace!("sent result successfully");
                        } else {
                            error!("failed to send result, receiver dropped?");
                        }
                    }
                    Err(_e) => error!("error sending msg"),
                }
            });
        }
    }
}
