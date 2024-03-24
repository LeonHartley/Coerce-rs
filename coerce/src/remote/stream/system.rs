use crate::remote::net::proto::network::{
    LeaderChangedEvent, MemberUpEvent, NewNodeEvent, NodeRemovedEvent, SystemEvent as SysEvent,
};
use crate::remote::net::StreamData;
use crate::remote::stream::pubsub::Topic;
use std::sync::Arc;

use crate::remote::system::NodeId;
use protobuf::{Enum, Error, Message};

use crate::remote::cluster::node::RemoteNodeRef;

pub struct SystemTopic;

#[derive(Debug)]
pub enum ClusterEvent {
    MemberUp(ClusterMemberUp),
    NodeAdded(RemoteNodeRef),
    NodeRemoved(RemoteNodeRef),
    LeaderChanged(NodeId),
}

#[derive(Debug)]
pub struct ClusterMemberUp {
    pub leader_id: NodeId,
    pub nodes: Vec<RemoteNodeRef>,
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

        SystemEvent::Cluster(ClusterEvent::NodeAdded(Arc::new(node.into())))
    }
}

impl From<NodeRemovedEvent> for SystemEvent {
    fn from(message: NodeRemovedEvent) -> Self {
        let node = message.node.unwrap();

        SystemEvent::Cluster(ClusterEvent::NodeRemoved(Arc::new(node.into())))
    }
}
impl From<LeaderChangedEvent> for SystemEvent {
    fn from(message: LeaderChangedEvent) -> Self {
        SystemEvent::Cluster(ClusterEvent::LeaderChanged(message.node_id))
    }
}

impl From<MemberUpEvent> for SystemEvent {
    fn from(e: MemberUpEvent) -> Self {
        SystemEvent::Cluster(ClusterEvent::MemberUp(ClusterMemberUp {
            leader_id: e.leader_id,
            nodes: e.nodes.into_iter().map(|n| Arc::new(n.into())).collect(),
        }))
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
                Some(SysEvent::ClusterMemberUp) => {
                    Some(MemberUpEvent::parse_from_bytes(message).unwrap().into())
                }
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
                        node: Some(node.as_ref().into()).into(),
                        ..Default::default()
                    };

                    write_event(SysEvent::ClusterNewNode, event.write_to_bytes())
                }

                ClusterEvent::NodeRemoved(node) => {
                    let event = NodeRemovedEvent {
                        node: Some(node.as_ref().into()).into(),
                        ..Default::default()
                    };

                    write_event(SysEvent::ClusterNodeRemoved, event.write_to_bytes())
                }

                ClusterEvent::LeaderChanged(node_id) => {
                    let event = LeaderChangedEvent {
                        node_id: *node_id,
                        ..Default::default()
                    };

                    write_event(SysEvent::ClusterLeaderChanged, event.write_to_bytes())
                }

                ClusterEvent::MemberUp(member_up) => {
                    let event = MemberUpEvent {
                        leader_id: member_up.leader_id,
                        nodes: member_up
                            .nodes
                            .iter()
                            .map(|n| n.as_ref().clone().into())
                            .collect(),
                        ..Default::default()
                    };

                    write_event(SysEvent::ClusterMemberUp, event.write_to_bytes())
                }
            },
        }
    }
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
