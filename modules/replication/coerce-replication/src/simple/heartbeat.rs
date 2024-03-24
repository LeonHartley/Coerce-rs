use crate::protocol::simple as proto;
use crate::simple::{Replicator, State};
use crate::storage::Storage;
use chrono::{DateTime, Utc};
use coerce::actor::context::ActorContext;
use coerce::actor::message::{
    FromBytes, Handler, Message, MessageUnwrapErr, MessageWrapErr, ToBytes,
};
use coerce::actor::scheduler::timer::TimerTick;
use coerce::remote::net::message::{datetime_to_timestamp, timestamp_to_datetime};
use protobuf::well_known_types::wrappers::UInt64Value;
use protobuf::{Message as ProtoMessage, MessageField};

#[derive(Debug, Clone)]
pub struct Heartbeat {
    source_node_id: u64,
    last_commit_index: Option<u64>,
    timestamp: DateTime<Utc>,
}

#[derive(Clone)]
pub struct HeartbeatTick;

#[async_trait]
impl<S: Storage> Handler<HeartbeatTick> for Replicator<S> {
    async fn handle(&mut self, _: HeartbeatTick, _ctx: &mut ActorContext) {
        match &self.state {
            State::Available { cluster, .. } => {
                let last_commit_index = self.storage.last_commit_index();

                let message = Heartbeat {
                    source_node_id: self.system.node_id(),
                    last_commit_index,
                    timestamp: Utc::now(),
                };

                cluster.broadcast(message).await;
            }

            _ => {}
        }
    }
}

#[async_trait]
impl<S: Storage> Handler<Heartbeat> for Replicator<S> {
    async fn handle(&mut self, message: Heartbeat, ctx: &mut ActorContext) {
        debug!(
            source_node_id = message.source_node_id,
            "received heartbeat"
        )
    }
}

impl Message for HeartbeatTick {
    type Result = ();
}

impl TimerTick for HeartbeatTick {}

impl Message for Heartbeat {
    type Result = ();

    fn as_bytes(&self) -> Result<Vec<u8>, MessageWrapErr> {
        proto::Heartbeat {
            source_node_id: self.source_node_id,
            last_commit_index: self.last_commit_index.map(|n| n.into()).into(),
            timestamp: Some(datetime_to_timestamp(&self.timestamp)).into(),
            ..Default::default()
        }
        .to_bytes()
    }

    fn from_bytes(buf: Vec<u8>) -> Result<Self, MessageUnwrapErr> {
        let proto = proto::Heartbeat::from_bytes(buf)?;
        Ok(Self {
            source_node_id: proto.source_node_id,
            last_commit_index: proto.last_commit_index.map(|n| n.value).into_option(),
            timestamp: timestamp_to_datetime(proto.timestamp.unwrap()),
        })
    }
}
