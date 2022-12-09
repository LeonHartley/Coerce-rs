use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message, MessageUnwrapErr, MessageWrapErr};
use crate::actor::{ActorId, ActorRef};
use crate::persistent::{PersistentActor, Recover};
use crate::remote::system::NodeId;
use crate::sharding::coordinator::{ShardCoordinator, ShardHostState, ShardId};
use crate::sharding::host::{ShardAllocated, ShardAllocator, ShardHost, ShardReallocating};
use crate::sharding::proto::sharding as proto;
use futures::future::join_all;
use protobuf::Message as ProtoMessage;
use std::collections::hash_map::{DefaultHasher, Entry, VacantEntry};

use crate::sharding::proto::sharding::allocate_shard_result;
use std::hash::{Hash, Hasher};

pub struct AllocateShard {
    pub shard_id: ShardId,
    pub rebalancing: bool,
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

    pub async fn allocate_shard(
        &mut self,
        shard_id: ShardId,
        _ctx: &mut ActorContext,
    ) -> AllocateShardResult {
        let shard_entry = self.shards.entry(shard_id);

        match shard_entry {
            Entry::Occupied(node) => AllocateShardResult::AlreadyAllocated(shard_id, *node.get()),
            Entry::Vacant(vacant) => {
                allocate(
                    shard_id,
                    self.hosts.values_mut().filter(|n| n.is_ready()).collect(),
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
        if let Some(entry) = self.shards.get(&message.shard_id) {
            return AllocateShardResult::AlreadyAllocated(message.shard_id, *entry);
        }

        if message.rebalancing {
            return self.allocate_shard(message.shard_id, ctx).await;
        }

        match self.persist(&message, ctx).await {
            Ok(_) => self.allocate_shard(message.shard_id, ctx).await,
            Err(e) => {
                warn!(
                    "error persisting a `AllocateShard`, shard_id={}, err={}",
                    message.shard_id, e
                );
                AllocateShardResult::Err(AllocateShardErr::Persistence)
            }
        }
    }
}

#[async_trait]
impl Recover<AllocateShard> for ShardCoordinator {
    async fn recover(&mut self, message: AllocateShard, ctx: &mut ActorContext) {
        trace!("recovered `AllocateShard`, shard_id={}", message.shard_id);

        self.allocate_shard(message.shard_id, ctx).await;
    }
}

async fn allocate(
    shard_id: ShardId,
    mut hosts: Vec<&mut ShardHostState>,
    shard_entry: VacantEntry<'_, ShardId, NodeId>,
) -> AllocateShardResult {
    // TODO: weighted ordering - shards with more entities should have a higher weight, the more shards*

    hosts.sort_by(|h1, h2| h1.shards.len().cmp(&h2.shards.len()));

    debug!(
        "shard#{} allocating - available nodes={:#?}",
        shard_id, &hosts
    );

    if let Some(host) = hosts.first_mut() {
        let node_id = host.node_id;

        trace!("shard#{} allocated, target_node={}", shard_id, node_id);

        shard_entry.insert(node_id);
        if host.shards.insert(shard_id) {
            let node_id = host.node_id;
            let hosts = hosts.iter().map(|h| h.actor.clone()).collect();
            tokio::spawn(async move {
                broadcast_allocation(shard_id, node_id, hosts).await;
            });
        }

        AllocateShardResult::Allocated(shard_id, node_id)
    } else {
        AllocateShardResult::NotAllocated
    }
}

pub async fn broadcast_allocation(
    shard_id: ShardId,
    node_id: NodeId,
    hosts: Vec<ActorRef<ShardHost>>,
) {
    trace!(
        "shard allocated (shard=#{}, node_id={}), broadcasting to all shard hosts",
        shard_id,
        node_id
    );
    let mut futures = vec![];

    for host in hosts.into_iter() {
        if host.node_id() == Some(node_id) {
            if let Err(_e) = host.send(ShardAllocated(shard_id, node_id)).await {
                warn!("")
            }
        } else {
            futures.push(async move {
                let host = host;

                trace!(
                    "emitting ShardAllocated to node_id={}",
                    host.node_id().unwrap_or(0)
                );

                if let Err(_e) = host.send(ShardAllocated(shard_id, node_id)).await {
                    error!(
                        "error attempting to send `ShardAllocated({}, {})` to {}",
                        shard_id, node_id, &host
                    )
                }
            });
        }
    }

    let _results = join_all(futures).await;
    trace!("broadcast to all nodes complete");
}

pub async fn broadcast_reallocation(shard_id: ShardId, hosts: Vec<ActorRef<ShardHost>>) {
    trace!(
        "shard reallocating (shard=#{}), broadcasting to all shard hosts",
        shard_id
    );
    let mut futures = vec![];

    for host in hosts {
        futures.push(async move {
            let host = host;

            trace!(
                "emitting ShardReallocating to node_id={}",
                host.node_id().unwrap_or(0)
            );

            host.send(ShardReallocating(shard_id)).await
        });
    }

    let _results = join_all(futures).await;
    trace!("broadcast to all nodes complete");
}

impl Message for AllocateShard {
    type Result = AllocateShardResult;

    fn as_bytes(&self) -> Result<Vec<u8>, MessageWrapErr> {
        proto::AllocateShard {
            shard_id: self.shard_id,
            rebalancing: self.rebalancing,
            ..Default::default()
        }
        .write_to_bytes()
        .map_or_else(|_| Err(MessageWrapErr::SerializationErr), |b| Ok(b))
    }

    fn from_bytes(buffer: Vec<u8>) -> Result<Self, MessageUnwrapErr> {
        proto::AllocateShard::parse_from_bytes(&buffer).map_or_else(
            |_| Err(MessageUnwrapErr::DeserializationErr),
            |allocate_shard| {
                Ok(AllocateShard {
                    shard_id: allocate_shard.shard_id,
                    rebalancing: allocate_shard.rebalancing,
                })
            },
        )
    }

    fn read_remote_result(buffer: Vec<u8>) -> Result<Self::Result, MessageUnwrapErr> {
        let result = proto::AllocateShardResult::parse_from_bytes(&buffer);
        let result = match result {
            Ok(result) => match result.result_type.unwrap() {
                allocate_shard_result::Type::ALLOCATED => {
                    let allocation = result.allocation.unwrap();
                    AllocateShardResult::Allocated(allocation.shard_id, allocation.node_id)
                }
                allocate_shard_result::Type::ALREADY_ALLOCATED => {
                    let allocation = result.allocation.unwrap();
                    AllocateShardResult::AlreadyAllocated(allocation.shard_id, allocation.node_id)
                }
                allocate_shard_result::Type::NOT_ALLOCATED => AllocateShardResult::NotAllocated,
                allocate_shard_result::Type::ERR => {
                    AllocateShardResult::Err(match result.err.unwrap() {
                        allocate_shard_result::AllocateShardErr::PERSISTENCE => {
                            AllocateShardErr::Persistence
                        }
                        allocate_shard_result::AllocateShardErr::UNKNOWN => {
                            AllocateShardErr::Unknown
                        }
                    })
                }
            },
            Err(_e) => return Err(MessageUnwrapErr::DeserializationErr),
        };

        Ok(result)
    }

    fn write_remote_result(res: Self::Result) -> Result<Vec<u8>, MessageWrapErr> {
        let mut result: proto::AllocateShardResult = Default::default();

        match res {
            AllocateShardResult::Allocated(shard_id, node_id) => {
                result.result_type = allocate_shard_result::Type::ALLOCATED.into();
                result.allocation = Some(proto::RemoteShard {
                    shard_id,
                    node_id,
                    ..Default::default()
                })
                .into();
            }
            AllocateShardResult::AlreadyAllocated(shard_id, node_id) => {
                result.result_type = allocate_shard_result::Type::ALREADY_ALLOCATED.into();
                result.allocation = Some(proto::RemoteShard {
                    shard_id,
                    node_id,
                    ..Default::default()
                })
                .into();
            }
            AllocateShardResult::NotAllocated => {
                result.result_type = allocate_shard_result::Type::NOT_ALLOCATED.into();
            }
            AllocateShardResult::Err(e) => {
                result.result_type = allocate_shard_result::Type::ERR.into();
                result.err = match e {
                    AllocateShardErr::Persistence => {
                        allocate_shard_result::AllocateShardErr::PERSISTENCE
                    }
                    AllocateShardErr::Unknown => allocate_shard_result::AllocateShardErr::UNKNOWN,
                }
                .into();
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
        DefaultAllocator { max_shards: 100 }
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
