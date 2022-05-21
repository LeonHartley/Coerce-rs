use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message};
use crate::actor::{ActorRefErr, LocalActorRef};
use crate::remote::cluster::sharding::coordinator::ShardId;
use crate::remote::cluster::sharding::host::ShardHost;
use crate::remote::cluster::sharding::shard::stats::{GetShardStats, ShardStats};
use crate::remote::cluster::sharding::shard::Shard;
use crate::remote::system::NodeId;
use futures::future::join_all;
use std::collections::HashMap;
use tokio::sync::oneshot;

pub struct GetStats;

#[derive(Serialize, Deserialize)]
pub struct RemoteShard {
    pub shard_id: ShardId,
    pub node_id: NodeId,
}

pub struct HostStats {
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
            .filter_map(|s| s.1.actor_ref())
            .collect();

        let stats = HostStats {
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
