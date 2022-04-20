use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message};
use crate::actor::{Actor, ActorRef};
use crate::remote::cluster::sharding::coordinator::allocation::{
    broadcast_reallocation, AllocateShard, AllocateShardResult,
};
use crate::remote::cluster::sharding::coordinator::{
    ShardCoordinator, ShardHostState, ShardHostStatus, ShardId,
};
use crate::remote::cluster::sharding::host::{ShardHost, StopShard};
use crate::remote::system::NodeId;
use futures::future::join_all;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::mem::replace;
use std::pin::Pin;
use std::task::{Context, Poll};

pub enum Rebalance {
    Shards(Vec<ShardId>),
    Node(NodeId),
}

#[async_trait]
impl Handler<Rebalance> for ShardCoordinator {
    async fn handle(&mut self, message: Rebalance, ctx: &mut ActorContext) {
        match message {
            Rebalance::Shards(shards) => {
                let mut shard_reallocation_tasks = vec![];
                let self_ref = self.actor_ref(ctx);
                let hosts = self
                    .hosts
                    .iter()
                    .map(|h| h.1.actor.clone())
                    .collect::<Vec<ActorRef<ShardHost>>>();

                for shard in shards.clone() {
                    tokio::spawn(broadcast_reallocation(shard, hosts.clone()));

                    if let Some(shard_node_id) = self.shards.remove(&shard) {
                        let shard_host = self.hosts.get_mut(&shard_node_id).unwrap();
                        let _ = shard_host.shards.remove(&shard);

                        let self_ref = self_ref.clone();
                        let shard_host_actor = shard_host.actor.clone();

                        shard_reallocation_tasks.push(async move {
                            let result = shard_host_actor.send(StopShard { shard_id: shard }).await;
                            if result.is_ok() {
                                let _ = self_ref.send(AllocateShard { shard_id: shard }).await;
                            } else {
                                let err = result.unwrap_err();
                                error!("error during shard rebalancing, ")
                            }
                        });
                    }
                }

                let shard_reallocation_tasks = shard_reallocation_tasks;
                let _ = tokio::spawn(async move {
                    join_all(shard_reallocation_tasks).await;

                    debug!("rebalance of shards ({:?}) complete", &shards)
                });
            }

            Rebalance::Node(node_id) => {
                debug!("beginning re-balance of shards hosted on node={}", node_id);

                let shards_to_rebalance = {
                    let mut shard_host_state = match self.hosts.get_mut(&node_id) {
                        None => return,
                        Some(shard_host_state) => shard_host_state,
                    };

                    // If the host is confirmed to be healthy, it can be marked as healthy later, which will
                    // enable shards to be allocated to it again
                    shard_host_state.status = ShardHostStatus::Unavailable;

                    replace(&mut shard_host_state.shards, HashSet::new())
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
    }
}

// #[async_trait]
// impl Handler<ShardsRebalanced> for ShardCoordinator {
//     async fn handle(&mut self, message: ShardsRebalanced, ctx: &mut ActorContext) {}
// }

impl Message for Rebalance {
    type Result = ();
}
