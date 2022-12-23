use crate::remote::net::StreamData;
use crate::remote::stream::pubsub::Topic;
use std::sync::Arc;

pub struct ShardingTopic {
    sharded_entity: Arc<str>,
}

pub enum ShardingEvent {
    Rebalance,
}

impl Topic for ShardingTopic {
    type Message = ShardingEvent;

    fn topic_name() -> &'static str {
        "sharding"
    }

    fn key(&self) -> String {
        format!("sharding-events-{}", &self.sharded_entity)
    }
}

impl StreamData for ShardingEvent {
    fn read_from_bytes(data: Vec<u8>) -> Option<Self> {
        todo!()
    }

    fn write_to_bytes(&self) -> Option<Vec<u8>> {
        todo!()
    }
}
