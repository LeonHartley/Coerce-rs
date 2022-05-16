use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message};
use crate::actor::{ActorRef, ActorRefErr, LocalActorRef};
use crate::remote::api::sharding::ShardingApi;
use crate::remote::cluster::sharding::coordinator::stats::{GetShardingStats, NodeStats};
use crate::remote::cluster::sharding::coordinator::ShardId;
use crate::remote::cluster::sharding::host::{shard_actor_id, GetCoordinator};
use crate::remote::cluster::sharding::shard::stats::GetShardStats;
use crate::remote::cluster::sharding::shard::stats::ShardStats as ShardActorStats;
use crate::remote::cluster::sharding::shard::Shard;
use crate::remote::system::NodeId;
use crate::remote::RemoteActorRef;
use axum::extract::Path;
use axum::response::IntoResponse;
use axum::Json;
use futures::future::join_all;
use tokio::sync::oneshot;

pub struct GetClusterStats {
    entity_type: String,
}

type ClusterStatsReceiver = oneshot::Receiver<Option<ShardingClusterStats>>;

impl Message for GetClusterStats {
    type Result = Option<ClusterStatsReceiver>;
}

#[derive(Serialize, Deserialize)]
pub struct ShardingNode {
    pub node_id: NodeId,
    pub shard_count: u64,
}

#[derive(Serialize, Deserialize)]
pub struct Entity {
    actor_id: String,
}

#[derive(Serialize, Deserialize)]
pub struct ShardStats {
    shard_id: ShardId,
    node_id: NodeId,
    entity_count: u32,
    entities: Vec<Entity>,
}

#[derive(Serialize, Deserialize)]
pub struct ShardingClusterStats {
    pub entity_type: String,
    pub total_shards: u64,
    pub total_nodes: u32,
    pub total_entities: u32,
    pub nodes: Vec<ShardingNode>,
    pub shards: Vec<ShardStats>,
}

#[async_trait]
impl Handler<GetClusterStats> for ShardingApi {
    async fn handle(
        &mut self,
        message: GetClusterStats,
        ctx: &mut ActorContext,
    ) -> Option<ClusterStatsReceiver> {
        let shard_host = self.shard_hosts.get(&message.entity_type);
        if shard_host.is_none() {
            return None;
        }

        let remote = ctx.system().remote_owned();
        let shard_host = shard_host.unwrap().clone();
        let (tx, rx) = oneshot::channel();

        tokio::spawn(async move {
            let remote = remote;
            let shard_host = shard_host;
            let coordinator = shard_host.send(GetCoordinator).await;

            if let Ok(coordinator) = coordinator {
                let sharding_stats = coordinator.send(GetShardingStats).await.unwrap();

                let mut shards: Vec<ActorRef<Shard>> = vec![];
                let self_node_id = remote.node_id();
                for shard in sharding_stats.shards {
                    let actor_id = shard_actor_id(&message.entity_type, shard.shard_id);

                    let shard_actor_ref = if shard.node_id == self_node_id {
                        let local_actor = remote.actor_system().get_tracked_actor(actor_id).await;
                        if let Some(actor) = local_actor {
                            actor.into()
                        } else {
                            warn!(
                                "could not find local shard actor (actor_id={})",
                                shard_actor_id(&message.entity_type, shard.shard_id)
                            );
                            continue;
                        }
                    } else {
                        RemoteActorRef::<Shard>::new(actor_id, shard.node_id, remote.clone()).into()
                    };

                    shards.push(shard_actor_ref);
                }

                let nodes: Vec<ShardingNode> =
                    sharding_stats.nodes.into_iter().map(|s| s.into()).collect();
                let results: Vec<Result<ShardActorStats, ActorRefErr>> =
                    join_all(shards.iter().map(|f| f.send(GetShardStats))).await;

                let shards: Vec<ShardStats> =
                    results.into_iter().map(|s| s.unwrap().into()).collect();

                let _ = tx.send(Some(ShardingClusterStats {
                    entity_type: sharding_stats.entity_type,
                    total_shards: sharding_stats.total_shards,
                    total_nodes: nodes.len() as u32,
                    total_entities: shards.iter().map(|s| s.entity_count).sum(),
                    nodes,
                    shards,
                }));
            }
        });

        Some(rx)
    }
}

pub async fn get_sharding_stats(
    sharding_api: LocalActorRef<ShardingApi>,
    Path(entity_type): Path<String>,
) -> impl IntoResponse {
    let cluster_stats: Option<ClusterStatsReceiver> = sharding_api
        .send(GetClusterStats { entity_type })
        .await
        .unwrap();
    if let Some(cluster_stats_receiver) = cluster_stats {
        Json(cluster_stats_receiver.await.unwrap())
    } else {
        Json(None)
    }
}

impl From<ShardActorStats> for ShardStats {
    fn from(s: ShardActorStats) -> Self {
        Self {
            shard_id: s.shard_id,
            entity_count: s.entities.len() as u32,
            node_id: s.node_id,
            entities: s
                .entities
                .into_iter()
                .map(|e| Entity { actor_id: e })
                .collect(),
        }
    }
}

impl From<NodeStats> for ShardingNode {
    fn from(node: NodeStats) -> Self {
        Self {
            node_id: node.node_id,
            shard_count: node.shard_count,
        }
    }
}
