use crate::remote::net::proto::protocol::{
    LeaderChangedEvent, NewNodeEvent, NodeRemovedEvent, SystemEvent as SysEvent,
};
use crate::remote::net::StreamData;
use crate::remote::stream::pubsub::Topic;

use crate::remote::system::NodeId;
use protobuf::{Message, ProtobufEnum, ProtobufError};

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
impl From<LeaderChangedEvent> for SystemEvent {
    fn from(message: LeaderChangedEvent) -> Self {
        SystemEvent::Cluster(ClusterEvent::LeaderChanged(message.get_node_id()))
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
                    Some(NewNodeEvent::parse_from_bytes(message).unwrap().into())
                }
                Some(SysEvent::ClusterLeaderChanged) => Some(
                    LeaderChangedEvent::parse_from_bytes(message)
                        .unwrap()
                        .into(),
                ),
                None => None,
            },
            None => None,
        }
    }

    fn write_to_bytes(&self) -> Option<Vec<u8>> {
        match self {
            SystemEvent::Cluster(cluster) => match cluster {
                ClusterEvent::NodeAdded(node_id) => {
                    let event = NewNodeEvent {
                        node_id: *node_id,
                        ..NewNodeEvent::default()
                    };

                    write_event(SysEvent::ClusterNewNode, event.write_to_bytes())
                }
                ClusterEvent::NodeRemoved(node_id) => {
                    let event = NodeRemovedEvent {
                        node_id: *node_id,
                        ..NodeRemovedEvent::default()
                    };

                    write_event(SysEvent::ClusterNodeRemoved, event.write_to_bytes())
                }
                ClusterEvent::LeaderChanged(node_id) => {
                    let event = LeaderChangedEvent {
                        node_id: *node_id,
                        ..LeaderChangedEvent::default()
                    };

                    write_event(SysEvent::ClusterLeaderChanged, event.write_to_bytes())
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
