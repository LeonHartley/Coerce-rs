use crate::remote::net::proto::protocol::{
    NewNodeEvent, NodeRemovedEvent, SystemEvent as SysEvent,
};
use crate::remote::net::StreamData;
use crate::remote::stream::pubsub::Topic;
use crate::remote::system::NodeId;
use protobuf::{Message, ProtobufEnum, ProtobufError};
use uuid::Uuid;

pub struct SystemTopic;

pub enum ClusterEvent {
    NodeAdded(NodeId),
    NodeRemoved(NodeId),
    LeaderChanged(NodeId),
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
        SystemEvent::Cluster(ClusterEvent::NodeAdded(message.get_node_id()))
    }
}

impl From<NodeRemovedEvent> for SystemEvent {
    fn from(message: NodeRemovedEvent) -> Self {
        SystemEvent::Cluster(ClusterEvent::NodeRemoved(message.get_node_id()))
    }
}

impl StreamData for SystemEvent {
    fn read_from_bytes(data: Vec<u8>) -> Option<Self> {
        match data.split_first() {
            Some((event, message)) => match SysEvent::from_i32(*event as i32) {
                Some(SysEvent::ClusterNodeRemoved) => {
                    Some(NodeRemovedEvent::parse_from_bytes(message).unwrap().into())
                }
                Some(SysEvent::ClusterNewNode) => {
                    Some(NodeRemovedEvent::parse_from_bytes(message).unwrap().into())
                }
                None => None,
            },
            None => None,
        }
    }

    fn write_to_bytes(self) -> Option<Vec<u8>> {
        match self {
            SystemEvent::Cluster(cluster) => match cluster {
                ClusterEvent::NodeAdded(id) => {
                    let event = NewNodeEvent {
                        node_id: id,
                        ..NewNodeEvent::default()
                    };

                    write_event(SysEvent::ClusterNewNode, event.write_to_bytes())
                }
                ClusterEvent::NodeRemoved(id) => {
                    let event = NodeRemovedEvent {
                        node_id: id,
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
            message.insert(0, system_event as u8);

            Some(message)
        }
        Err(_) => None,
    };

    o
}
