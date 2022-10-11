use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message, MessageUnwrapErr, MessageWrapErr};

use crate::remote::cluster::sharding::coordinator::ShardId;

use crate::remote::cluster::sharding::proto::sharding as proto;
use crate::remote::cluster::sharding::shard::Shard;
use crate::remote::system::NodeId;
use protobuf::Message as ProtoMessage;
use std::collections::HashSet;

#[derive(Serialize, Deserialize)]
pub struct GetShardStats;

#[derive(Serialize, Deserialize)]
pub struct ShardStats {
    pub shard_id: ShardId,
    pub node_id: NodeId,
    pub entities: HashSet<String>,
}

#[async_trait]
impl Handler<GetShardStats> for Shard {
    async fn handle(&mut self, _message: GetShardStats, ctx: &mut ActorContext) -> ShardStats {
        let node_id = ctx.system().remote().node_id();
        let shard_id = self.shard_id;

        ShardStats {
            shard_id,
            node_id,
            entities: self.entities.keys().map(|e| e.to_string()).collect(),
        }
    }
}

impl Message for GetShardStats {
    type Result = ShardStats;

    fn as_bytes(&self) -> Result<Vec<u8>, MessageWrapErr> {
        proto::GetShardStats {
            ..Default::default()
        }
        .write_to_bytes()
        .map_or_else(|_e| Err(MessageWrapErr::SerializationErr), |m| Ok(m))
    }

    fn from_bytes(_: Vec<u8>) -> Result<Self, MessageUnwrapErr> {
        Ok(Self)
    }

    fn read_remote_result(res: Vec<u8>) -> Result<Self::Result, MessageUnwrapErr> {
        proto::ShardStats::parse_from_bytes(&res).map_or_else(
            |_| Err(MessageUnwrapErr::DeserializationErr),
            |s| {
                Ok(ShardStats {
                    shard_id: s.shard_id,
                    node_id: s.node_id,
                    entities: s.entities.into_iter().map(|e| e.to_string()).collect(),
                })
            },
        )
    }

    fn write_remote_result(res: ShardStats) -> Result<Vec<u8>, MessageWrapErr> {
        proto::ShardStats {
            shard_id: res.shard_id,
            node_id: res.node_id,
            entities: res.entities.into_iter().map(|e| e.to_string()).collect(),
            ..Default::default()
        }
        .write_to_bytes()
        .map_or_else(|_| Err(MessageWrapErr::SerializationErr), |b| Ok(b))
    }
}
