use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message};
use crate::actor::{Actor, ActorRef, LocalActorRef};
use crate::remote::cluster::sharding::coordinator::allocation::{
    broadcast_reallocation, AllocateShard,
};
use crate::remote::cluster::sharding::coordinator::{ShardCoordinator, ShardHostStatus, ShardId};
use crate::remote::cluster::sharding::host::{ShardHost, StopShard};
use crate::remote::system::NodeId;
use futures::future::join_all;

use std::mem;
use std::time::Instant;
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
                self.rebalance_shards(self.shards.keys().copied().collect(), self_ref)
                    .await
            }

            Rebalance::Shards(shards) => {
                let self_ref = ctx.actor_ref();
                self.rebalance_shards(shards, self_ref).await
            }

            Rebalance::NodeUnavailable(node_id) => {
                self.rebalance_unavailable_node(node_id, ctx).await
            }
        }

        warn!(
            "Rebalance took {:?}, rebalance={}",
            start.elapsed(),
            rebalance
        );
    }
}

impl ShardCoordinator {
    pub async fn rebalance_shards(&mut self, shards: Vec<ShardId>, self_ref: LocalActorRef<Self>) {
        let mut shard_reallocation_tasks = vec![];
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
                    let result = shard_host_actor.send(StopShard { shard_id: shard, request_id: Uuid::new_v4() }).await;
                    match result {
                        Ok(_) => {
                            let _ = self_ref.send(AllocateShard { shard_id: shard }).await;
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

            debug!("rebalance of shards ({:?}) complete", &shards)
        });
    }

    pub async fn rebalance_unavailable_node(&mut self, node_id: NodeId, ctx: &mut ActorContext) {
        debug!("beginning re-balance of shards hosted on node={}", node_id);

        let shards_to_rebalance = {
            let mut shard_host_state = match self.hosts.get_mut(&node_id) {
                None => return,
                Some(shard_host_state) => shard_host_state,
            };

            // If the host is confirmed to be healthy, it can be marked as healthy later, which will
            // enable shards to be allocated to it again
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
