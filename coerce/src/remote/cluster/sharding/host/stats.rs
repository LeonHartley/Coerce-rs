use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message};
use crate::remote::cluster::sharding::coordinator::ShardId;
use crate::remote::cluster::sharding::host::ShardHost;
use crate::remote::system::NodeId;

pub struct GetStats;

pub struct RemoteShard {
    pub shard_id: ShardId,
    pub node_id: NodeId,
}

pub struct HostStats {
    pub hosted_shards: Vec<ShardId>,
    pub remote_shards: Vec<RemoteShard>,
}

impl Message for GetStats {
    type Result = HostStats;
}

#[async_trait]
impl Handler<GetStats> for ShardHost {
    async fn handle(&mut self, _message: GetStats, _ctx: &mut ActorContext) -> HostStats {
        HostStats {
            hosted_shards: self.hosted_shards.keys().copied().collect(),
            remote_shards: self
                .remote_shards
                .iter()
                .map(|s| RemoteShard {
                    node_id: s.1.node_id().unwrap(),
                    shard_id: *s.0,
                })
                .collect(),
        }
    }
}
