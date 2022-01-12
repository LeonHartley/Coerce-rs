use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message};
use crate::actor::ActorId;
use crate::remote::cluster::sharding::coordinator::ShardId;
use crate::remote::cluster::sharding::host::ShardHost;
use crate::remote::cluster::sharding::shard::Shard;
use crate::remote::system::NodeId;
use std::collections::HashSet;

pub struct GetShardStats;

pub struct ShardStats {
    pub entities: HashSet<ActorId>,
}

impl Message for GetShardStats {
    type Result = ShardStats;
}

#[async_trait]
impl Handler<GetShardStats> for Shard {
    async fn handle(&mut self, _message: GetShardStats, _ctx: &mut ActorContext) -> ShardStats {
        ShardStats {
            entities: self.entities.keys().cloned().collect(),
        }
    }
}
