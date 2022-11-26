use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message};
use crate::actor::{ActorRef, LocalActorRef};
use crate::remote::system::{NodeId, RemoteActorSystem};
use crate::sharding::coordinator::allocation::{broadcast_reallocation, AllocateShard};
use crate::sharding::coordinator::{ShardCoordinator, ShardHostStatus, ShardId};
use crate::sharding::host::{ShardHost, ShardStopped, StopShard};
use futures::future::join_all;

use std::mem;
use std::time::Instant;
use tokio::sync::oneshot;
use uuid::Uuid;

#[derive(Debug)]
pub enum Rebalance {
    Shards(Vec<ShardId>),
    NodeUnavailable(NodeId),
    All,
}

#[async_trait]
impl Handler<Rebalance> for ShardCoordinator {
    async fn handle(&mut self, message: Rebalance, ctx: &mut ActorContext) {
        let start = Instant::now();
        let rebalance = format!("{:?}", &message);

        match message {
            Rebalance::All => {
                let self_ref = ctx.actor_ref();

                let total_shards = self.shards.len();
                let fair_shard_count_per_node = total_shards / self.hosts.len();

                let mut shards_to_rebalance = vec![];
                for (node_id, shard_host) in &self.hosts {
                    if shard_host.shards.len() > fair_shard_count_per_node {
                        let diff = shard_host.shards.len() - fair_shard_count_per_node;
                        let mut i = 0;
                        for shard in &shard_host.shards {
                            if i >= diff {
                                break;
                            }
                            shards_to_rebalance.push(*shard);

                            i += 1;
                        }

                        debug!("rebalancing {} shards from node={} - total={} - fair_shard_count_per_node={}", i, node_id, shard_host.shards.len(), fair_shard_count_per_node);
                    }
                }

                self.rebalance_shards(shards_to_rebalance, self_ref, ctx.system().remote())
                    .await
            }

            Rebalance::Shards(shards) => {
                let self_ref = ctx.actor_ref();
                self.rebalance_shards(shards, self_ref, ctx.system().remote())
                    .await
            }

            Rebalance::NodeUnavailable(node_id) => {
                self.rebalance_unavailable_node(node_id, ctx).await
            }
        }

        // TODO: if the rebalance took over a configurable amount of time, warnings/errors/alerts could be fired
        debug!(
            "Rebalance took {:?}, rebalance={}",
            start.elapsed(),
            rebalance
        );
    }
}

impl ShardCoordinator {
    pub async fn rebalance_shards(
        &mut self,
        shards: Vec<ShardId>,
        self_ref: LocalActorRef<Self>,
        system: &RemoteActorSystem,
    ) {
        let mut shard_reallocation_tasks = vec![];
        let hosts = self
            .hosts
            .iter()
            .map(|h| h.1.actor.clone())
            .collect::<Vec<ActorRef<ShardHost>>>();

        let origin_node_id = self.self_node_id.expect("node id");
        for shard in shards.clone() {
            tokio::spawn(broadcast_reallocation(shard, hosts.clone()));

            if let Some(shard_node_id) = self.shards.remove(&shard) {
                let shard_host = self.hosts.get_mut(&shard_node_id).unwrap();
                let _ = shard_host.shards.remove(&shard);

                let self_ref = self_ref.clone();
                let shard_host_actor = shard_host.actor.clone();

                let sys = system.clone();
                shard_reallocation_tasks.push(async move {
                    let request_id = Uuid::new_v4();

                    let (tx, rx) = oneshot::channel();
                    sys.push_request(request_id, tx);

                    let stop = StopShard { shard_id: shard, request_id, origin_node_id };
                    let result = shard_host_actor.notify(stop).await;
                    match result {
                        Ok(_) => {
                            let result = rx.await;
                            if let Ok(result) = result {
                                let result = ShardStopped::from_bytes(result.into_result().unwrap());
                                match result {
                                    Ok(_res) => {
                                        let _ = self_ref.send(AllocateShard { shard_id: shard, rebalancing: true }).await;
                                    },
                                    Err(e) => {
                                        error!("deserialization error: {}", e);
                                    },
                                }
                            } else {
                                warn!("error recv")
                            }

                        }
                        Err(e) => {
                            error!("error during shard re-balancing - failed to send StopShard (shard_id={}), target_node={}) error={}", shard, &shard_host_actor, e);
                        }
                    }
                });
            }
        }

        let shard_reallocation_tasks = shard_reallocation_tasks;
        let _ = tokio::spawn(async move {
            join_all(shard_reallocation_tasks).await;

            info!("rebalance of shards ({:?}) complete", &shards)
        });
    }

    pub async fn rebalance_unavailable_node(&mut self, node_id: NodeId, ctx: &mut ActorContext) {
        debug!("beginning re-balance of shards hosted on node={}", node_id);

        let shards_to_rebalance = {
            let mut shard_host_state = match self.hosts.get_mut(&node_id) {
                None => return,
                Some(shard_host_state) => shard_host_state,
            };

            // Mark the host as unavailable, which will exclude it from shard allocation.
            // The node can be made available again, once confirmed
            shard_host_state.status = ShardHostStatus::Unavailable;

            mem::take(&mut shard_host_state.shards)
        };

        let shard_count = shards_to_rebalance.len();
        for shard in shards_to_rebalance {
            self.shards.remove(&shard);
            let _ = self.allocate_shard(shard, ctx).await;
        }

        debug!(
            "re-balance of shards hosted on node={} complete, {} shards re-allocated",
            node_id, shard_count,
        );
    }
}

impl Message for Rebalance {
    type Result = ();
}
