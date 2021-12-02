use crate::actor::context::ActorContext;
use crate::actor::message::{
    Envelope, EnvelopeType, Handler, Message, MessageUnwrapErr, MessageWrapErr,
};
use crate::actor::{ActorRef, LocalActorRef};
use crate::persistent::{PersistentActor, Recover};
use crate::remote::cluster::sharding::coordinator::{ShardCoordinator, ShardHostState, ShardId};
use crate::remote::cluster::sharding::host::{ShardAllocated, ShardHost};
use crate::remote::system::NodeId;
use futures::future::join_all;
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

    fn as_remote_envelope(&self) -> Result<Envelope<Self>, MessageWrapErr> {
        Ok(Envelope::<Self>::Remote(
            self.shard_id.to_be_bytes().to_vec(),
        ))
    }
}

impl ShardCoordinator {
    fn shards_by_node(&self, node_id: NodeId) -> Option<Vec<ShardId>> {
        self.hosts
            .get(&node_id)
            .map(|n| n.shards.iter().copied().collect())
    }

    async fn allocate_shard(
        &mut self,
        shard: AllocateShard,
        ctx: &mut ActorContext,
    ) -> Option<NodeId> {
        let shard_entry = self.shards.entry(shard.shard_id);

        match shard_entry {
            Entry::Occupied(node) => Some(*node.get()),
            Entry::Vacant(vacant) => {
                allocate(
                    ctx.actor_ref(),
                    shard.shard_id,
                    self.hosts.values_mut().collect(),
                    vacant,
                )
                .await
            }
        }
    }
}

async fn allocate(
    coordinator: LocalActorRef<ShardCoordinator>,
    shard_id: ShardId,
    mut hosts: Vec<&mut ShardHostState>,
    shard_entry: VacantEntry<'_, ShardId, NodeId>,
) -> Option<NodeId> {
    hosts.sort_by(|h1, h2| h1.shards.len().cmp(&h2.shards.len()));

    if let Some(host) = hosts.first_mut() {
        let node_id = host.node_id;

        shard_entry.insert(node_id);
        if host.shards.insert(shard_id) {
            tokio::spawn(broadcast_allocation(
                coordinator,
                shard_id,
                host.node_id,
                hosts.iter().map(|h| h.actor.clone()).collect(),
            ));
        }

        Some(node_id)
    } else {
        None
    }
}

async fn broadcast_allocation(
    _coordinator: LocalActorRef<ShardCoordinator>,
    shard_id: ShardId,
    node_id: NodeId,
    hosts: Vec<ActorRef<ShardHost>>,
) {
    trace!(target: "ShardCoordinator", "shard allocated (shard=#{}, node_id={}), broadcasting to all shard hosts", shard_id, node_id);
    let mut futures = vec![];

    for host in hosts.into_iter() {
        // TODO: apply timeout
        futures.push(async move {
            let host = host;

            host.send(ShardAllocated(shard_id, node_id)).await
        });
    }

    let _results = join_all(futures).await;
    trace!(target: "ShardCoordinator", "broadcast to all nodes complete");
}

#[async_trait]
impl Handler<AllocateShard> for ShardCoordinator {
    async fn handle(
        &mut self,
        message: AllocateShard,
        ctx: &mut ActorContext,
    ) -> AllocateShardResult {
        if self.persist(&message, ctx).await.is_ok() {
            if let Some(node_id) = self.allocate_shard(message, ctx).await {
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
    async fn recover(&mut self, message: AllocateShard, ctx: &mut ActorContext) {
        self.allocate_shard(message, ctx).await;
    }
}
