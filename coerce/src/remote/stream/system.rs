use crate::remote::net::proto::protocol::{
    NewNodeEvent, NodeRemovedEvent, SystemEvent as SysEvent,
};
use crate::remote::net::StreamMessage;
use crate::remote::stream::pubsub::Topic;
use protobuf::{parse_from_bytes, Message, ProtobufEnum, ProtobufError};
use uuid::Uuid;

pub struct SystemTopic;

pub enum ClusterEvent {
    NodeAdded(Uuid),
    NodeRemoved(Uuid),
}

pub enum SystemEvent {
    Cluster(ClusterEvent),
}

impl Topic for SystemTopic {
    type Message = SystemEvent;

    fn topic_name() -> &'static str {
        "coerce"
    }
}

impl From<NewNodeEvent> for SystemEvent {
    fn from(message: NewNodeEvent) -> Self {
        SystemEvent::Cluster(ClusterEvent::NodeAdded(
            Uuid::parse_str(message.get_node_id()).unwrap(),
        ))
    }
}

impl From<NodeRemovedEvent> for SystemEvent {
    fn from(message: NodeRemovedEvent) -> Self {
        SystemEvent::Cluster(ClusterEvent::NodeRemoved(
            Uuid::parse_str(message.get_node_id()).unwrap(),
        ))
    }
}

impl StreamMessage for SystemEvent {
    fn read_from_bytes(data: Vec<u8>) -> Option<Self> {
        match data.split_first() {
            Some((event, message)) => match SysEvent::from_i32(*event as i32) {
                Some(SysEvent::ClusterNodeRemoved) => Some(
                    parse_from_bytes::<NodeRemovedEvent>(message)
                        .unwrap()
                        .into(),
                ),
                Some(SysEvent::ClusterNewNode) => {
                    Some(parse_from_bytes::<NewNodeEvent>(message).unwrap().into())
                }
                None => None,
            },
            None => None,
        }
    }

    fn write_to_bytes(&self) -> Option<Vec<u8>> {
        match self {
            SystemEvent::Cluster(cluster) => match cluster {
                ClusterEvent::NodeAdded(id) => {
                    let event = NewNodeEvent {
                        node_id: id.to_string(),
                        ..NewNodeEvent::default()
                    };

                    write_event(SysEvent::ClusterNewNode, event.write_to_bytes())
                }
                ClusterEvent::NodeRemoved(id) => {
                    let event = NodeRemovedEvent {
                        node_id: id.to_string(),
                        ..NodeRemovedEvent::default()
                    };

                    write_event(SysEvent::ClusterNodeRemoved, event.write_to_bytes())
                }
            },
        }
    }
}

fn write_event(system_event: SysEvent, message: Result<Vec<u8>, ProtobufError>) -> Option<Vec<u8>> {
    let o = match message {
        Ok(mut message) => {
            let mut bytes = vec![];
            bytes.push(system_event as u8);
            bytes.append(&mut message);

            Some(bytes)
        }
        Err(_) => None,
    };

    o
}
