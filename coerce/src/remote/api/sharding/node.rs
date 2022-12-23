use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message};
use crate::actor::LocalActorRef;
use crate::remote::api::sharding::ShardingApi;

use crate::sharding::host::stats::GetStats as GetHostStats;
use axum::extract::Path;
use axum::response::IntoResponse;
use axum::Json;
use std::collections::HashMap;

#[derive(Serialize, Deserialize)]
pub struct GetStats(pub String);

#[derive(Serialize, Deserialize)]
pub struct GetAllStats;

#[derive(Serialize, Deserialize)]
pub struct RebalanceEntity(pub String);

#[derive(Serialize, Deserialize, ToSchema)]
pub struct Stats {
    entity: String,
    total_shards: usize,
    hosted_shards: HashMap<u32, ShardStats>,
    remote_shards: HashMap<u32, u64>,
}

#[derive(Serialize, Deserialize, ToSchema)]
pub struct ShardStats {
    pub shard_id: u32,
    pub node_id: u64,
    pub entities: Vec<String>,
}
impl Message for GetStats {
    type Result = Option<Stats>;
}

impl Message for GetAllStats {
    type Result = HashMap<String, Stats>;
}

#[async_trait]
impl Handler<GetStats> for ShardingApi {
    async fn handle(&mut self, message: GetStats, _ctx: &mut ActorContext) -> Option<Stats> {
        if let Some(host) = self.shard_hosts.get(&message.0) {
            let host_stats = host.send(GetHostStats).await;
            if let Ok(host_stats_receiver) = host_stats {
                if let Ok(host_stats) = host_stats_receiver.await {
                    let total_shards =
                        host_stats.hosted_shard_count + host_stats.remote_shard_count;

                    Some(Stats {
                        entity: message.0,
                        hosted_shards: HashMap::new(), /*host_stats.hosted_shards,*/
                        remote_shards: HashMap::new(), /*host_stats
                                                       .remote_shards
                                                       .into_iter()
                                                       .map(|s| (s.shard_id, s.node_id))
                                                       .collect(),*/
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
                        host_stats.hosted_shard_count + host_stats.remote_shard_count;

                    stats.insert(
                        entity.clone(),
                        Stats {
                            entity: entity.clone(),
                            hosted_shards: HashMap::new(), /*host_stats.hosted_shards*/
                            remote_shards: HashMap::new(), /*host_stats
                                                           .remote_shards
                                                           .into_iter()
                                                           .map(|s| (s.shard_id, s.node_id))
                                                           .collect()*/
                            total_shards,
                        },
                    );
                }
            }
        }

        stats
    }
}

#[utoipa::path(
    get,
    path = "/sharding/stats/node/{entity}",
    responses(
        (status = 200, description = "ShardHost stats for the the chosen entity type", body = Stats),
    ),
    params(
        ("entity" = String, Path, description = "Sharded entity type name"),
    )
)]
pub async fn get_node_stats(
    actor_ref: LocalActorRef<ShardingApi>,
    Path(entity): Path<String>,
) -> impl IntoResponse {
    Json(
        actor_ref
            .send(GetStats(entity))
            .await
            .expect("unable to get stats"),
    )
}
