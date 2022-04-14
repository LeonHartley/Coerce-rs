use crate::actor::context::ActorContext;
use crate::actor::message::{
    Envelope, EnvelopeType, Handler, Message, MessageUnwrapErr, MessageWrapErr,
};
use crate::actor::{ActorId, ActorRef, LocalActorRef};
use crate::persistent::{PersistentActor, Recover};
use crate::remote::cluster::sharding::coordinator::{ShardCoordinator, ShardHostState, ShardId};
use crate::remote::cluster::sharding::host::{ShardAllocated, ShardAllocator, ShardHost};
use crate::remote::cluster::sharding::proto::sharding as proto;
use crate::remote::cluster::sharding::proto::sharding::{
    AllocateShardResult_AllocateShardErr, AllocateShardResult_Type,
};
use crate::remote::system::NodeId;
use futures::future::join_all;
use protobuf::{Message as ProtoMessage, SingularPtrField};
use std::collections::hash_map::{DefaultHasher, Entry, VacantEntry};
use std::convert::TryInto;
use std::hash::{Hash, Hasher};

pub struct AllocateShard {
    pub shard_id: ShardId,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum AllocateShardErr {
    Unknown,
    Persistence,
}

#[derive(Debug, Eq, PartialEq)]
pub enum AllocateShardResult {
    Allocated(ShardId, NodeId),
    AlreadyAllocated(ShardId, NodeId),
    NotAllocated,
    Err(AllocateShardErr),
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
    ) -> AllocateShardResult {
        let shard_entry = self.shards.entry(shard.shard_id);

        match shard_entry {
            Entry::Occupied(node) => {
                AllocateShardResult::AlreadyAllocated(shard.shard_id, *node.get())
            }
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

#[async_trait]
impl Handler<AllocateShard> for ShardCoordinator {
    async fn handle(
        &mut self,
        message: AllocateShard,
        ctx: &mut ActorContext,
    ) -> AllocateShardResult {
        if self.persist(&message, ctx).await.is_ok() {
            self.allocate_shard(message, ctx).await
        } else {
            warn!(target: "ShardCoordinator", "error persisting a `AllocateShard`, shard_id={}", message.shard_id);
            AllocateShardResult::Err(AllocateShardErr::Persistence)
        }
    }
}

#[async_trait]
impl Recover<AllocateShard> for ShardCoordinator {
    async fn recover(&mut self, message: AllocateShard, ctx: &mut ActorContext) {
        trace!(target: "ShardCoordinator", "recovered `AllocateShard`, shard_id={}", message.shard_id);

        self.allocate_shard(message, ctx).await;
    }
}

async fn allocate(
    coordinator: LocalActorRef<ShardCoordinator>,
    shard_id: ShardId,
    mut hosts: Vec<&mut ShardHostState>,
    shard_entry: VacantEntry<'_, ShardId, NodeId>,
) -> AllocateShardResult {
    hosts.sort_by(|h1, h2| h1.shards.len().cmp(&h2.shards.len()));

    debug!(target: "ShardCoordinator", "shard#{} allocating - available nodes={}", shard_id, hosts.len());

    if let Some(host) = hosts.first_mut() {
        let node_id = host.node_id;

        trace!(target: "ShardCoordinator", "shard#{} allocated, target_node={}", shard_id, node_id);

        shard_entry.insert(node_id);
        if host.shards.insert(shard_id) {
            tokio::spawn(broadcast_allocation(
                coordinator,
                shard_id,
                host.node_id,
                hosts.iter().map(|h| h.actor.clone()).collect(),
            ));
        }

        AllocateShardResult::Allocated(shard_id, node_id)
    } else {
        AllocateShardResult::NotAllocated
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
        if host.node_id() == Some(node_id) {
            host.send(ShardAllocated(shard_id, node_id)).await;
        } else {
            futures.push(async move {
                let host = host;

                trace!(
                    "emitting ShardAllocated to node_id={}",
                    host.node_id().unwrap_or(0)
                );
                host.send(ShardAllocated(shard_id, node_id)).await
            });
        }
    }

    let _results = join_all(futures).await;
    trace!(target: "ShardCoordinator", "broadcast to all nodes complete");
}

impl Message for AllocateShard {
    type Result = AllocateShardResult;

    fn as_remote_envelope(&self) -> Result<Envelope<Self>, MessageWrapErr> {
        proto::AllocateShard {
            shard_id: self.shard_id,
            ..Default::default()
        }
        .write_to_bytes()
        .map_or_else(
            |_| Err(MessageWrapErr::SerializationErr),
            |b| Ok(Envelope::Remote(b)),
        )
    }

    fn from_remote_envelope(buffer: Vec<u8>) -> Result<Self, MessageUnwrapErr> {
        proto::AllocateShard::parse_from_bytes(&buffer).map_or_else(
            |_| Err(MessageUnwrapErr::DeserializationErr),
            |allocate_shard| {
                Ok(AllocateShard {
                    shard_id: allocate_shard.shard_id,
                })
            },
        )
    }

    fn read_remote_result(buffer: Vec<u8>) -> Result<Self::Result, MessageUnwrapErr> {
        let result = proto::AllocateShardResult::parse_from_bytes(&buffer);
        let result = match result {
            Ok(result) => match result.result_type {
                AllocateShardResult_Type::ALLOCATED => {
                    let allocation = result.allocation.unwrap();
                    AllocateShardResult::Allocated(allocation.shard_id, allocation.node_id)
                }
                AllocateShardResult_Type::ALREADY_ALLOCATED => {
                    let allocation = result.allocation.unwrap();
                    AllocateShardResult::AlreadyAllocated(allocation.shard_id, allocation.node_id)
                }
                AllocateShardResult_Type::NOT_ALLOCATED => AllocateShardResult::NotAllocated,
                AllocateShardResult_Type::ERR => AllocateShardResult::Err(match result.err {
                    AllocateShardResult_AllocateShardErr::PERSISTENCE => {
                        AllocateShardErr::Persistence
                    }
                    AllocateShardResult_AllocateShardErr::UNKNOWN => AllocateShardErr::Unknown,
                }),
            },
            Err(_e) => return Err(MessageUnwrapErr::DeserializationErr),
        };

        Ok(result)
    }

    fn write_remote_result(res: Self::Result) -> Result<Vec<u8>, MessageWrapErr> {
        let mut result: proto::AllocateShardResult = Default::default();

        match res {
            AllocateShardResult::Allocated(shard_id, node_id) => {
                result.result_type = AllocateShardResult_Type::ALLOCATED;
                result.allocation = Some(proto::RemoteShard {
                    shard_id,
                    node_id,
                    ..Default::default()
                })
                .into();
            }
            AllocateShardResult::AlreadyAllocated(shard_id, node_id) => {
                result.result_type = AllocateShardResult_Type::ALREADY_ALLOCATED;
                result.allocation = Some(proto::RemoteShard {
                    shard_id,
                    node_id,
                    ..Default::default()
                })
                .into();
            }
            AllocateShardResult::NotAllocated => {
                result.result_type = AllocateShardResult_Type::NOT_ALLOCATED;
            }
            AllocateShardResult::Err(e) => {
                result.result_type = AllocateShardResult_Type::ERR;
                result.err = match e {
                    AllocateShardErr::Persistence => {
                        AllocateShardResult_AllocateShardErr::PERSISTENCE
                    }
                    AllocateShardErr::Unknown => AllocateShardResult_AllocateShardErr::UNKNOWN,
                }
            }
        }

        result
            .write_to_bytes()
            .map_err(|_e| MessageWrapErr::SerializationErr)
    }
}

pub struct DefaultAllocator {
    pub max_shards: ShardId,
}

impl Default for DefaultAllocator {
    fn default() -> Self {
        DefaultAllocator { max_shards: 1000 }
    }
}

impl ShardAllocator for DefaultAllocator {
    fn allocate(&mut self, actor_id: &ActorId) -> ShardId {
        let hashed_actor_id = {
            let mut hasher = DefaultHasher::new();
            actor_id.hash(&mut hasher);
            hasher.finish()
        };

        (hashed_actor_id % self.max_shards as u64) as ShardId
    }
}
