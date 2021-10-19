use crate::actor::context::{ActorContext, ActorStatus};
use crate::actor::message::{Handler, Message};
use crate::actor::{Actor, ActorId, LocalActorRef, ActorRefErr};
use crate::remote::cluster::sharding::coordinator::ShardId;
use crate::remote::cluster::sharding::shard::Shard;
use crate::remote::system::NodeId;
use std::collections::{HashMap, HashSet};

struct ShardState {
    actor: LocalActorRef<Shard>,
}

pub struct ShardHost {
    shard_entity: String,
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

impl Message for ShardAllocated {
    type Result = ();
}

impl Message for StopShard {
    type Result = ();
}

impl Message for StartEntity {
    type Result = ();
}

#[async_trait]
impl Handler<ShardAllocated> for ShardHost {
    async fn handle(&mut self, message: ShardAllocated, ctx: &mut ActorContext) {
        if message.node_id == ctx.system().remote().node_id() {
            // start shard
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
                ActorStatus::Stopped => {},
                _ => {},
            },
            Err(_) => {}
        }
    }
}
