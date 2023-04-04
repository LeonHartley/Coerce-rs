use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message};
use crate::actor::{ActorRef, LocalActorRef};
use crate::remote::api::sharding::ShardingApi;
use crate::remote::system::RemoteActorSystem;
use crate::remote::RemoteActorRef;
use crate::sharding::coordinator::stats::{GetShardingStats, NodeStats};
use crate::sharding::host::stats::{GetStats, HostStats, RemoteShard};
use crate::sharding::host::{shard_actor_id, GetCoordinator};
use crate::sharding::shard::stats::GetShardStats;
use crate::sharding::shard::stats::ShardStats as ShardActorStats;
use crate::sharding::shard::Shard;
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

pub struct GetHostStats {
    entity_type: String,
}

type HostStatsReceiver = oneshot::Receiver<Option<HostStats>>;

impl Message for GetHostStats {
    type Result = Option<HostStatsReceiver>;
}

#[derive(Serialize, ToSchema)]
pub enum ShardHostStatus {
    Unknown,
    Starting,
    Ready,
    Unavailable,
}

impl ShardHostStatus {
    pub fn is_available(&self) -> bool {
        matches!(&self, Self::Ready)
    }
}

#[derive(Serialize, ToSchema)]
pub struct ShardingNode {
    pub node_id: u64,
    pub shard_count: u64,
    pub status: ShardHostStatus,
}

#[derive(Serialize, ToSchema)]
pub struct Entity {
    actor_id: String,
}

#[derive(Serialize, ToSchema)]
pub struct ShardStats {
    shard_id: u32,
    node_id: u64,
    entity_count: u32,
    entities: Vec<Entity>,
}

#[derive(Serialize, ToSchema)]
pub struct ShardingClusterStats {
    pub entity_type: String,
    pub total_shards: u64,
    pub total_nodes: u32,
    pub available_nodes: u32,
    pub total_entities: u32,
    pub nodes: Vec<ShardingNode>,
    pub shards: Vec<ShardStats>,
}

#[async_trait]
impl Handler<GetHostStats> for ShardingApi {
    async fn handle(
        &mut self,
        message: GetHostStats,
        _ctx: &mut ActorContext,
    ) -> Option<HostStatsReceiver> {
        let shard_host = self.shard_hosts.get(&message.entity_type);
        if shard_host.is_none() {
            return None;
        }

        let shard_host = shard_host.unwrap().clone();

        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            let receiver = shard_host.send(GetStats).await;
            if let Ok(receiver) = receiver {
                let result = receiver.await;
                if let Ok(stats) = result {
                    let _ = tx.send(Some(stats));
                } else {
                    let _ = tx.send(None);
                }
            } else {
                let _ = tx.send(None);
            }
        });

        Some(rx)
    }
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
                let shards: Vec<ActorRef<Shard>> =
                    get_shards(&message.entity_type, &remote, sharding_stats.shards).await;
                let nodes: Vec<ShardingNode> =
                    sharding_stats.nodes.into_iter().map(|s| s.into()).collect();
                let shards: Vec<ShardStats> =
                    join_all(shards.iter().map(|f| f.send(GetShardStats)))
                        .await
                        .into_iter()
                        .map(|s| {
                            s /*todo: this unwrap may not be safe..*/
                                .unwrap()
                                .into()
                        })
                        .collect();

                let _ = tx.send(Some(ShardingClusterStats {
                    entity_type: sharding_stats.entity_type,
                    total_shards: sharding_stats.total_shards,
                    total_nodes: nodes.len() as u32,
                    available_nodes: nodes.iter().filter(|n| n.status.is_available()).count()
                        as u32,
                    total_entities: shards.iter().map(|s| s.entity_count).sum(),
                    nodes,
                    shards,
                }));
            } else {
                let _ = tx.send(None);
            }
        });

        Some(rx)
    }
}

async fn get_shards(
    shard_entity: &String,
    remote: &RemoteActorSystem,
    shards: Vec<RemoteShard>,
) -> Vec<ActorRef<Shard>> {
    let mut actor_refs = vec![];
    for shard in shards {
        let actor_id = shard_actor_id(shard_entity, shard.shard_id);
        let shard_actor_ref = if shard.node_id == remote.node_id() {
            // TODO: Can we grab the refs directly from the ShardHost rather than having to look them up one by one?
            //       OR can we grab a batch of refs from ActorScheduler?

            let local_actor = remote.actor_system().get_tracked_actor(actor_id).await;
            if let Some(actor) = local_actor {
                actor.into()
            } else {
                warn!(
                    "could not find local shard actor (actor_id={})",
                    shard_actor_id(&shard_entity, shard.shard_id)
                );
                continue;
            }
        } else {
            RemoteActorRef::<Shard>::new(actor_id, shard.node_id, remote.clone()).into()
        };

        actor_refs.push(shard_actor_ref);
    }

    actor_refs
}

#[utoipa::path(
    get,
    path = "/sharding/stats/cluster/{entity}",
    responses(
        (status = 200, description = "Sharding stats for the the chosen entity type", body = ShardingClusterStats),
    ),
    params(
        ("entity" = String, Path, description = "Sharded entity type name"),
    )
)]
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

pub async fn get_shard_host_stats(
    sharding_api: LocalActorRef<ShardingApi>,
    Path(entity_type): Path<String>,
) -> impl IntoResponse {
    let host_stats: Option<HostStatsReceiver> = sharding_api
        .send(GetHostStats { entity_type })
        .await
        .unwrap();
    if let Some(host_stats) = host_stats {
        Json(host_stats.await.unwrap())
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
                .map(|e| Entity {
                    actor_id: e.to_string(),
                })
                .collect(),
        }
    }
}

impl From<crate::sharding::coordinator::ShardHostStatus> for ShardHostStatus {
    fn from(value: crate::sharding::coordinator::ShardHostStatus) -> Self {
        match value {
            crate::sharding::coordinator::ShardHostStatus::Unknown => Self::Unknown,
            crate::sharding::coordinator::ShardHostStatus::Starting => Self::Starting,
            crate::sharding::coordinator::ShardHostStatus::Ready => Self::Ready,
            crate::sharding::coordinator::ShardHostStatus::Unavailable => Self::Unavailable,
        }
    }
}

impl From<NodeStats> for ShardingNode {
    fn from(node: NodeStats) -> Self {
        Self {
            node_id: node.node_id,
            shard_count: node.shard_count,
            status: node.status.into(),
        }
    }
}
