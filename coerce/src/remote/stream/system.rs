use crate::remote::net::proto::network::{
    LeaderChangedEvent, NewNodeEvent, NodeRemovedEvent, SystemEvent as SysEvent,
};
use crate::remote::net::StreamData;
use crate::remote::stream::pubsub::Topic;
use std::sync::Arc;

use crate::remote::cluster::node::RemoteNode;

use crate::remote::net::message::{datetime_to_timestamp, timestamp_to_datetime};
use crate::remote::net::proto::network as proto;
use crate::remote::system::NodeId;
use protobuf::{Enum, Error, Message};

pub struct SystemTopic;

#[derive(Debug)]
pub enum ClusterEvent {
    NodeAdded(Arc<RemoteNode>),
    NodeRemoved(Arc<RemoteNode>),
    LeaderChanged(NodeId),
}

#[derive(Debug)]
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
        let node = message.node.unwrap();

        SystemEvent::Cluster(ClusterEvent::NodeAdded(Arc::new(RemoteNode {
            id: node.node_id,
            addr: node.addr,
            tag: node.tag,
            node_started_at: node
                .node_started_at
                .into_option()
                .map(|d| timestamp_to_datetime(d)),
        })))
    }
}

impl From<NodeRemovedEvent> for SystemEvent {
    fn from(message: NodeRemovedEvent) -> Self {
        let node = message.node.unwrap();

        SystemEvent::Cluster(ClusterEvent::NodeRemoved(Arc::new(RemoteNode {
            id: node.node_id,
            addr: node.addr,
            tag: node.tag,
            node_started_at: node
                .node_started_at
                .into_option()
                .map(|d| timestamp_to_datetime(d)),
        })))
    }
}
impl From<LeaderChangedEvent> for SystemEvent {
    fn from(message: LeaderChangedEvent) -> Self {
        SystemEvent::Cluster(ClusterEvent::LeaderChanged(message.node_id))
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
                ClusterEvent::NodeAdded(node) => {
                    let event = NewNodeEvent {
                        node: Some(get_proto_node(node.as_ref())).into(),
                        ..NewNodeEvent::default()
                    };

                    write_event(SysEvent::ClusterNewNode, event.write_to_bytes())
                }
                ClusterEvent::NodeRemoved(node) => {
                    let event = NodeRemovedEvent {
                        node: Some(get_proto_node(node.as_ref())).into(),
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

fn get_proto_node(node: &RemoteNode) -> proto::RemoteNode {
    proto::RemoteNode {
        node_id: node.id,
        addr: node.addr.clone(),
        tag: node.tag.clone(),
        node_started_at: node
            .node_started_at
            .as_ref()
            .map(datetime_to_timestamp)
            .into(),
        ..proto::RemoteNode::default()
    }
    .into()
}

fn write_event(system_event: SysEvent, message: Result<Vec<u8>, Error>) -> Option<Vec<u8>> {
    let o = match message {
        Ok(mut message) => {
            message.insert(0, system_event as u8);

            Some(message)
        }
        Err(_) => None,
    };

    o
}
