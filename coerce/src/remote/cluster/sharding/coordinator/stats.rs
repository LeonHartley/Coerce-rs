use crate::actor::context::ActorContext;
use crate::actor::message::{Envelope, Handler, Message, MessageUnwrapErr, MessageWrapErr};
use crate::remote::cluster::sharding::coordinator::{ShardCoordinator, ShardHostStatus};
use crate::remote::cluster::sharding::host::stats::RemoteShard;
use crate::remote::cluster::sharding::proto::sharding as proto;
use crate::remote::system::NodeId;
use protobuf::{EnumOrUnknown, Message as ProtoMessage};

pub struct GetShardingStats;

pub struct NodeStats {
    pub node_id: NodeId,
    pub shard_count: u64,
    pub status: ShardHostStatus,
}

pub struct ShardingStats {
    pub entity_type: String,
    pub total_shards: u64,
    pub shards: Vec<RemoteShard>,
    pub nodes: Vec<NodeStats>,
}

#[async_trait]
impl Handler<GetShardingStats> for ShardCoordinator {
    async fn handle(&mut self, _msg: GetShardingStats, _ctx: &mut ActorContext) -> ShardingStats {
        ShardingStats {
            entity_type: self.shard_entity.clone(),
            total_shards: self.shards.len() as u64,
            shards: self
                .shards
                .iter()
                .map(|s| RemoteShard {
                    shard_id: *s.0,
                    node_id: *s.1,
                })
                .collect(),
            nodes: self
                .hosts
                .iter()
                .map(|n| NodeStats {
                    node_id: n.1.node_id,
                    shard_count: n.1.shards.len() as u64,
                    status: n.1.status,
                })
                .collect(),
        }
    }
}

impl Message for GetShardingStats {
    type Result = ShardingStats;

    fn as_bytes(&self) -> Result<Vec<u8>, MessageWrapErr> {
        proto::GetShardingStats {
            ..Default::default()
        }
        .write_to_bytes()
        .map_err(|_| MessageWrapErr::SerializationErr)
    }

    fn from_bytes(bytes: Vec<u8>) -> Result<Self, MessageUnwrapErr> {
        proto::GetShardingStats::parse_from_bytes(&bytes).map_or_else(
            |_| Err(MessageUnwrapErr::DeserializationErr),
            |_m| Ok(GetShardingStats),
        )
    }

    fn read_remote_result(res: Vec<u8>) -> Result<Self::Result, MessageUnwrapErr> {
        let proto_stats = proto::ShardingStats::parse_from_bytes(&res);
        if let Ok(stats) = proto_stats {
            Ok(ShardingStats {
                entity_type: stats.entity_type,
                total_shards: stats.total_shards,
                shards: stats
                    .shards
                    .into_iter()
                    .map(|s| RemoteShard {
                        shard_id: s.shard_id,
                        node_id: s.node_id,
                    })
                    .collect(),
                nodes: stats
                    .nodes
                    .into_iter()
                    .map(|n| NodeStats {
                        node_id: n.node_id,
                        shard_count: n.shard_count,
                        status: n.status.unwrap().into(),
                    })
                    .collect(),
            })
        } else {
            Err(MessageUnwrapErr::DeserializationErr)
        }
    }

    fn write_remote_result(res: ShardingStats) -> Result<Vec<u8>, MessageWrapErr> {
        proto::ShardingStats {
            entity_type: res.entity_type,
            total_shards: res.total_shards,
            shards: res
                .shards
                .into_iter()
                .map(|s| proto::RemoteShard {
                    shard_id: s.shard_id,
                    node_id: s.node_id,
                    ..Default::default()
                })
                .collect(),
            nodes: res
                .nodes
                .into_iter()
                .map(|n| proto::NodeStats {
                    node_id: n.node_id,
                    shard_count: n.shard_count,
                    status: EnumOrUnknown::new(n.status.into()),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
        .write_to_bytes()
        .map_or_else(|_| Err(MessageWrapErr::SerializationErr), |b| Ok(b))
    }
}

impl Into<proto::ShardHostStatus> for ShardHostStatus {
    fn into(self) -> proto::ShardHostStatus {
        match self {
            ShardHostStatus::Unknown => proto::ShardHostStatus::UNKNOWN,
            ShardHostStatus::Starting => proto::ShardHostStatus::STARTING,
            ShardHostStatus::Ready => proto::ShardHostStatus::READY,
            ShardHostStatus::Unavailable => proto::ShardHostStatus::UNAVAILABLE,
        }
    }
}

impl From<proto::ShardHostStatus> for ShardHostStatus {
    fn from(status: proto::ShardHostStatus) -> Self {
        match status {
            proto::ShardHostStatus::UNKNOWN => Self::Unknown,
            proto::ShardHostStatus::STARTING => Self::Starting,
            proto::ShardHostStatus::READY => Self::Ready,
            proto::ShardHostStatus::UNAVAILABLE => Self::Unavailable,
        }
    }
}
