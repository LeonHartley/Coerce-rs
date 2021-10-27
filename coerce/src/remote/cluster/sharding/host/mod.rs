use crate::actor::context::{ActorContext, ActorStatus};
use crate::actor::message::{Handler, Message};
use crate::actor::{Actor, ActorId, ActorRefErr, IntoActor, IntoChild, LocalActorRef};
use crate::remote::cluster::sharding::coordinator::ShardId;
use crate::remote::cluster::sharding::shard::Shard;
use crate::remote::system::NodeId;
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::{Hash, Hasher};
use tokio::sync::oneshot::Sender;
use chrono::{DateTime, Utc};

struct ShardState {
    actor: LocalActorRef<Shard>,
}

pub struct ShardHost {
    shard_entity: String,
    max_shards: ShardId,
    hosted_shards: HashMap<ShardId, ShardState>,
    remote_shards: HashMap<ShardId, NodeId>,
}

impl Actor for ShardHost {}

pub struct ShardAllocated {
    shard_id: ShardId,
    node_id: NodeId,
}

pub struct StopShard {
    shard_id: ShardId,
}

pub struct StartEntity {
    pub actor_id: ActorId,
    pub recipe: Vec<u8>,
}

pub struct EntityRequest {
    pub actor_id: ActorId,
    pub message_type: String,
    pub message: Vec<u8>,
    pub recipe: Option<Vec<u8>>,
    pub result_channel: Option<Sender<Result<Vec<u8>, ActorRefErr>>>
}

impl Message for ShardAllocated {
    type Result = ();
}

impl Message for StopShard {
    type Result = ();
}

impl Message for StartEntity {
    type Result = ();
}

impl Message for EntityRequest {
    type Result = ();
}

// TODO: Allow this to be overridden
pub fn calculate_shard_id(actor_id: &ActorId, max_shards: ShardId) -> ShardId {
    let hashed_actor_id = {
        let mut hasher = DefaultHasher::new();
        actor_id.hash(&mut hasher);
        hasher.finish()
    };

    (hashed_actor_id % max_shards as u64) as ShardId
}

#[async_trait]
impl Handler<EntityRequest> for ShardHost {
    async fn handle(&mut self, message: EntityRequest, ctx: &mut ActorContext) {
        let shard_id = calculate_shard_id(&message.actor_id, self.max_shards);

        if let Some(shard) = self.hosted_shards.get(&shard_id) {
            let actor = shard.actor.clone();
            tokio::spawn(async move {
                let actor_id = message.actor_id.clone();
                let message_type = message.message_type.clone();

                let result = actor.send(message).await;
                if !result.is_ok() {
                    error!(
                        "failed to deliver EntityRequest (actor_id={}, type={}) to shard (shard_id={})",
                        &actor_id, &message_type, shard_id
                    );
                } else {
                    trace!(
                        "delivered EntityRequest (actor_id={}, type={}) to shard (shard_id={})",
                        &actor_id,
                        message_type,
                        shard_id
                    );
                }
            });
        } else if let Some(shard) = self.remote_shards.get(&shard_id) {
        } else {
            // TODO: unallocated shard -> ask coordinator for the shard allocation

        }
    }
}

#[async_trait]
impl Handler<ShardAllocated> for ShardHost {
    async fn handle(&mut self, message: ShardAllocated, ctx: &mut ActorContext) {
        let remote = ctx.system().remote();
        if message.node_id == remote.node_id() {
            let handler = remote
                .config()
                .actor_handler(&self.shard_entity)
                .expect("actor factory not supported");

            let shard = Shard::new(message.shard_id, handler)
                .into_child(Some(format!("Shard-{}", message.shard_id)), ctx)
                .await
                .expect("create shard actor");

            self.hosted_shards
                .insert(message.shard_id, ShardState { actor: shard });
        } else {
            self.remote_shards.insert(message.shard_id, message.node_id);
        }
    }
}

#[async_trait]
impl Handler<StopShard> for ShardHost {
    async fn handle(&mut self, message: StopShard, ctx: &mut ActorContext) {
        // TODO: having the shard host wait for the shard to stop
        //       will hurt throughput of other shards during re-balancing,
        //       need a way to defer it and return a remote result via oneshot channel

        let status = match self.hosted_shards.remove(&message.shard_id) {
            None => Ok(ActorStatus::Stopped),
            Some(shard) => shard.actor.stop().await,
        };

        match status {
            Ok(status) => match status {
                ActorStatus::Stopped => {}
                _ => {}
            },
            Err(_) => {}
        }
    }
}
