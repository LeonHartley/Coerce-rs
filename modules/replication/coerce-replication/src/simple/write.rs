use crate::simple::error::Error;
use crate::simple::{Replicator, Request, State};
use crate::storage::{Key, Storage, Value};
use coerce::actor::context::ActorContext;
use coerce::actor::message::{Handler, Message};
use coerce::actor::ActorRef;
use coerce::remote::system::{NodeId, RemoteActorSystem};
use std::collections::HashSet;
use tokio::sync::oneshot;

pub struct Write<K: Key, V: Value> {
    pub key: K,
    pub value: V,
    pub on_completion: Option<oneshot::Sender<Result<(), Error>>>,
}

pub struct UncommittedMutation<K: Key, V: Value> {
    pub mutation: Mutation<K, V>,
    pub votes: HashSet<NodeId>,
    pub on_completion: MutationCompletion,
}

pub struct Mutation<K: Key, V: Value> {
    pub log_index: u64,
    pub key: K,
    pub value: V,
}

pub enum MutationCompletion {
    Local(Option<oneshot::Sender<Result<(), Error>>>),
    Remote {
        request_id: u64,
        source_node_id: NodeId,
    },
}

impl MutationCompletion {
    pub fn notify_err(self, error: Error, system: &RemoteActorSystem) {
        self.notify(Err(error), system)
    }

    pub fn notify(self, result: Result<(), Error>, system: &RemoteActorSystem) {
        match self {
            MutationCompletion::Local(mut channel) => {
                let channel = channel.take().unwrap();
                let _ = channel.send(result);
            }

            MutationCompletion::Remote {
                request_id,
                source_node_id,
            } => {
                let result = match result {
                    Ok(_) => RemoteWriteResult::Ok(()),
                    Err(e) => RemoteWriteResult::Err(e),
                };

                let bytes = result.as_bytes().unwrap();
                let sys = system.clone();

                tokio::spawn(async move {
                    sys.notify_raw_rpc_result(request_id, bytes, source_node_id)
                        .await
                });
            }
        }
    }
}

impl<K: Key, V: Value> Message for Write<K, V> {
    type Result = ();
}

#[async_trait]
impl<S: Storage> Handler<Write<S::Key, S::Value>> for Replicator<S> {
    async fn handle(&mut self, mut write: Write<S::Key, S::Value>, _ctx: &mut ActorContext) {
        match &mut self.state {
            State::Joining { request_buffer } => {
                request_buffer.push_back(Request::Write(write));

                debug!(
                    pending_requests = request_buffer.len(),
                    "replicator still joining cluster, buffered write request"
                );
            }

            State::Recovering { cluster } | State::Available { cluster, .. } => {
                tokio::spawn(remote_write(
                    cluster.leader_actor.clone(),
                    write.key,
                    write.value,
                    write.on_completion.take().unwrap(),
                    self.system.clone(),
                ));
            }

            State::Leader { .. } => {
                // create a log entry
                // notify all nodes in cluster
                // upon ack, commit log entry
                // tell all nodes the latest commit index
            }
            _ => {}
        }
    }
}

pub struct RemoteWrite<K: Key, V: Value> {
    pub request_id: u64,
    pub source_node_id: u64,
    pub key: K,
    pub value: V,
}

impl<K: Key, V: Value> Message for RemoteWrite<K, V> {
    type Result = ();
}

pub enum RemoteWriteResult {
    Ok(()),
    Err(Error),
}

impl Message for RemoteWriteResult {
    type Result = ();
}

async fn remote_write<S: Storage>(
    leader_ref: ActorRef<Replicator<S>>,
    key: S::Key,
    value: S::Value,
    on_completion: oneshot::Sender<Result<(), Error>>,
    system: RemoteActorSystem,
) {
}
