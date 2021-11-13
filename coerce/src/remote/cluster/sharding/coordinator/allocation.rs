use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message};
use crate::persistent::{PersistentActor, Recover};
use crate::remote::cluster::sharding::coordinator::{ShardCoordinator, ShardHostState, ShardId};
use crate::remote::system::NodeId;
use std::collections::hash_map::{Entry, VacantEntry};

pub struct AllocateShard {
    pub shard_id: ShardId,
}

#[derive(Clone, Debug)]
pub enum AllocateShardErr {
    Persistence,
}

pub enum AllocateShardResult {
    Allocated(NodeId),
    NotAllocated,
    Err(AllocateShardErr),
}

impl Message for AllocateShard {
    type Result = AllocateShardResult;
}

impl ShardCoordinator {
    fn shards_by_node(&self, node_id: NodeId) -> Option<Vec<ShardId>> {
        self.hosts
            .get(&node_id)
            .map(|n| n.shards.iter().copied().collect())
    }

    async fn allocate_shard(&mut self, shard: AllocateShard) -> Option<NodeId> {
        let shard_entry = self.shards.entry(shard.shard_id);

        match shard_entry {
            Entry::Occupied(node) => Some(*node.get()),
            Entry::Vacant(vacant) => {
                allocate(shard.shard_id, self.hosts.values_mut().collect(), vacant).await
            }
        }
    }
}

async fn allocate(
    shard_id: ShardId,
    mut hosts: Vec<&mut ShardHostState>,
    shard_entry: VacantEntry<'_, ShardId, NodeId>,
) -> Option<NodeId> {
    hosts.sort_by(|h1, h2| h1.shards.len().cmp(&h2.shards.len()));

    if let Some(host) = hosts.first_mut() {
        let node_id = host.node_id;

        shard_entry.insert(node_id);
        if host.shards.insert(shard_id) {
            // TODO: emit update to all nodes, wait for ACK from all/majority
        }

        Some(node_id)
    } else {
        None
    }
}

#[async_trait]
impl Handler<AllocateShard> for ShardCoordinator {
    async fn handle(
        &mut self,
        message: AllocateShard,
        ctx: &mut ActorContext,
    ) -> AllocateShardResult {
        if self.persist(&message, ctx).await.is_ok() {
            if let Some(node_id) = self.allocate_shard(message).await {
                AllocateShardResult::Allocated(node_id)
            } else {
                AllocateShardResult::NotAllocated
            }
        } else {
            AllocateShardResult::Err(AllocateShardErr::Persistence)
        }
    }
}

#[async_trait]
impl Recover<AllocateShard> for ShardCoordinator {
    async fn recover(&mut self, message: AllocateShard, _ctx: &mut ActorContext) {
        self.allocate_shard(message).await;
    }
}
