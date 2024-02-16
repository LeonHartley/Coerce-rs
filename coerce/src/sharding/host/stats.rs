use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message};
use crate::actor::{ActorRefErr, LocalActorRef};
use crate::remote::system::NodeId;
use crate::sharding::coordinator::ShardId;
use crate::sharding::host::ShardHost;
use crate::sharding::shard::stats::{GetShardStats, ShardStats};
use crate::sharding::shard::Shard;
use futures::future::join_all;
use std::collections::HashMap;
use tokio::sync::oneshot;

pub struct GetStats;

#[derive(Serialize, Deserialize)]
pub struct RemoteShard {
    pub shard_id: ShardId,
    pub node_id: NodeId,
}

#[derive(Serialize, Deserialize)]
pub struct HostStats {
    pub requests_pending_shard_allocation_count: usize,
    pub hosted_shard_count: usize,
    pub remote_shard_count: usize,
    pub hosted_shards: HashMap<ShardId, ShardStats>,
    pub remote_shards: Vec<RemoteShard>,
}

impl Message for GetStats {
    type Result = oneshot::Receiver<HostStats>;
}

#[async_trait]
impl Handler<GetStats> for ShardHost {
    async fn handle(
        &mut self,
        _message: GetStats,
        _ctx: &mut ActorContext,
    ) -> oneshot::Receiver<HostStats> {
        let (tx, rx) = oneshot::channel();
        let actors: Vec<LocalActorRef<Shard>> = self
            .hosted_shards
            .iter()
            .filter_map(|s| s.1.actor_ref().cloned())
            .collect();

        let stats = HostStats {
            requests_pending_shard_allocation_count: self.requests_pending_shard_allocation.len(),
            hosted_shard_count: self.hosted_shards.len(),
            remote_shard_count: self.remote_shards.len(),
            hosted_shards: Default::default(),
            remote_shards: self
                .remote_shards
                .iter()
                .map(|s| RemoteShard {
                    node_id: s.1.node_id().unwrap(),
                    shard_id: *s.0,
                })
                .collect(),
        };

        tokio::spawn(async move {
            let mut stats = stats;
            let shard_stats: Vec<Result<ShardStats, ActorRefErr>> =
                join_all(actors.iter().map(|a| a.send(GetShardStats))).await;

            stats.hosted_shards = shard_stats
                .into_iter()
                .filter(|s| s.is_ok())
                .map(|s| {
                    let s = s.unwrap();
                    (s.shard_id, s)
                })
                .collect();

            let _ = tx.send(stats);
        });

        rx
    }
}
