use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message};
use crate::remote::api::sharding::ShardingApi;
use crate::remote::cluster::sharding::coordinator::ShardId;
use crate::remote::cluster::sharding::host::stats::GetStats as GetHostStats;
use crate::remote::cluster::sharding::shard::stats::ShardStats;
use crate::remote::system::NodeId;
use std::collections::HashMap;

#[derive(Serialize, Deserialize)]
pub struct GetShardTypes;

#[derive(Serialize, Deserialize)]
pub struct ShardTypes {
    entities: Vec<String>,
}

impl Message for GetShardTypes {
    type Result = ShardTypes;
}

#[derive(Serialize, Deserialize)]
pub struct GetStats(pub String);

#[derive(Serialize, Deserialize)]
pub struct GetAllStats;

#[derive(Serialize, Deserialize)]
pub struct Stats {
    entity: String,
    total_shards: usize,
    hosted_shards: HashMap<ShardId, ShardStats>,
    remote_shards: HashMap<ShardId, NodeId>,
}

impl Message for GetStats {
    type Result = Option<Stats>;
}

impl Message for GetAllStats {
    type Result = HashMap<String, Stats>;
}

#[async_trait]
impl Handler<GetShardTypes> for ShardingApi {
    async fn handle(&mut self, _message: GetShardTypes, _ctx: &mut ActorContext) -> ShardTypes {
        ShardTypes {
            entities: self.shard_hosts.keys().cloned().collect(),
        }
    }
}

#[async_trait]
impl Handler<GetStats> for ShardingApi {
    async fn handle(&mut self, message: GetStats, _ctx: &mut ActorContext) -> Option<Stats> {
        if let Some(host) = self.shard_hosts.get(&message.0) {
            let host_stats = host.send(GetHostStats).await;
            if let Ok(host_stats_receiver) = host_stats {
                if let Ok(host_stats) = host_stats_receiver.await {
                    let total_shards =
                        host_stats.hosted_shards.len() + host_stats.remote_shards.len();

                    Some(Stats {
                        entity: message.0,
                        hosted_shards: host_stats.hosted_shards,
                        remote_shards: host_stats
                            .remote_shards
                            .into_iter()
                            .map(|s| (s.shard_id, s.node_id))
                            .collect(),
                        total_shards,
                    })
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        }
    }
}

#[async_trait]
impl Handler<GetAllStats> for ShardingApi {
    async fn handle(
        &mut self,
        _message: GetAllStats,
        _ctx: &mut ActorContext,
    ) -> HashMap<String, Stats> {
        let mut stats = HashMap::new();
        for (entity, host) in &self.shard_hosts {
            let host_stats = host.send(GetHostStats).await;
            if let Ok(host_stats_receiver) = host_stats {
                if let Ok(host_stats) = host_stats_receiver.await {
                    let total_shards =
                        host_stats.hosted_shards.len() + host_stats.remote_shards.len();

                    stats.insert(
                        entity.clone(),
                        Stats {
                            entity: entity.clone(),
                            hosted_shards: host_stats.hosted_shards,
                            remote_shards: host_stats
                                .remote_shards
                                .into_iter()
                                .map(|s| (s.shard_id, s.node_id))
                                .collect(),
                            total_shards,
                        },
                    );
                }
            }
        }

        stats
    }
}
