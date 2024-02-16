use crate::remote::system::NodeId;

use hashring::HashRing;

use crate::remote::config::SystemCapabilities;
use crate::remote::net::message::{datetime_to_timestamp, timestamp_to_datetime};
use crate::remote::net::proto::network;
use crate::remote::stream::system::ClusterEvent::NodeAdded;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::fmt::Display;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;

pub struct RemoteNodeStore {
    nodes: HashMap<NodeId, RemoteNodeState>,
    table: HashRing<RemoteNode>,
}

#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize)]
pub enum NodeStatus {
    Joining,
    Healthy,
    Unhealthy,
    Terminated,
}

impl NodeStatus {
    pub fn is_healthy(&self) -> bool {
        return matches!(&self, Self::Healthy);
    }
}

pub type NodeAttribute = (Arc<str>, Arc<str>);

pub type NodeAttributes = HashMap<Arc<str>, Arc<str>>;

pub type NodeAttributesRef = Arc<NodeAttributes>;

#[derive(Debug, Clone)]
pub struct RemoteNodeState {
    pub id: NodeId,
    pub addr: String,
    pub tag: String,
    pub ping_latency: Option<Duration>,
    pub last_heartbeat: Option<DateTime<Utc>>,
    pub node_started_at: Option<DateTime<Utc>>,
    pub status: NodeStatus,
    pub attributes: NodeAttributesRef,
}

#[derive(Debug, Clone)]
pub struct RemoteNode {
    pub id: NodeId,
    pub addr: String,
    pub tag: String,
    pub node_started_at: Option<DateTime<Utc>>,
    pub attributes: NodeAttributesRef,
}

pub enum NodeSelector {
    All,
    Attribute(NodeAttribute),
}

impl NodeSelector {
    pub fn from_attribute(key: Arc<str>, value: Arc<str>) -> Self {
        Self::Attribute((key, value))
    }

    pub fn includes(&self, node: &RemoteNode) -> bool {
        match &self {
            NodeSelector::All => true,
            NodeSelector::Attribute((key, value)) => node.attributes.get(key) == Some(value),
        }
    }
}

pub type RemoteNodeRef = Arc<RemoteNode>;

impl Hash for RemoteNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
        self.addr.hash(state);
        self.tag.hash(state);
        self.node_started_at.hash(state);
    }
}

#[derive(Clone)]
pub struct NodeIdentity {
    pub node: RemoteNode,
    pub peers: Vec<RemoteNode>,
    pub capabilities: SystemCapabilities,
}

impl RemoteNodeStore {
    pub fn new(nodes: Vec<RemoteNode>) -> RemoteNodeStore {
        let mut table = HashRing::new();

        let nodes = nodes
            .into_iter()
            .map(|n| {
                table.add(n.clone());
                (n.id, RemoteNodeState::new(n))
            })
            .collect();

        RemoteNodeStore { table, nodes }
    }

    pub fn update_nodes(&mut self, nodes: Vec<RemoteNodeState>) {
        for node in nodes {
            self.nodes.insert(node.id, node);
        }
    }

    pub fn node_terminated(&mut self, node_id: NodeId) {
        let node = self.get_mut(&node_id);
        if let Some(node) = node {
            node.status = NodeStatus::Terminated;
        }
    }

    pub fn get(&self, node_id: &NodeId) -> Option<&RemoteNodeState> {
        self.nodes.get(node_id)
    }

    pub fn get_mut(&mut self, node_id: &NodeId) -> Option<&mut RemoteNodeState> {
        self.nodes.get_mut(node_id)
    }

    pub fn is_registered(&self, node_id: NodeId) -> bool {
        self.nodes.contains_key(&node_id)
    }

    pub fn remove(&mut self, node_id: &NodeId) -> Option<RemoteNode> {
        self.nodes.remove(&node_id).and_then(|node| {
            let node = node.into();
            self.table.remove(&node)
        })
    }

    pub fn get_by_key(&mut self, key: impl Hash) -> Option<&RemoteNode> {
        self.table.get(&key)
    }

    pub fn add(&mut self, node: RemoteNode) {
        let mut nodes = self.get_all();
        nodes.push(RemoteNodeState::new(node));
        nodes.sort_by(|a, b| a.id.partial_cmp(&b.id).unwrap());

        self.table = HashRing::new();
        self.nodes = nodes
            .into_iter()
            .map(|n| {
                let node = n.clone();
                self.table.add(n.into());
                (node.id, node)
            })
            .collect();
    }

    pub fn get_all(&self) -> Vec<RemoteNodeState> {
        self.nodes.values().cloned().collect()
    }
}

impl RemoteNodeState {
    pub fn new(node: RemoteNode) -> Self {
        let id = node.id;
        let addr = node.addr;
        let node_started_at = node.node_started_at;
        let tag = node.tag;

        Self {
            id,
            addr,
            node_started_at,
            tag,
            ping_latency: None,
            last_heartbeat: None,
            status: NodeStatus::Joining,
            attributes: node.attributes.clone(),
        }
    }
}

impl From<RemoteNodeState> for RemoteNode {
    fn from(s: RemoteNodeState) -> Self {
        Self {
            id: s.id,
            addr: s.addr,
            tag: s.tag,
            node_started_at: s.node_started_at,
            attributes: s.attributes,
        }
    }
}

impl From<network::RemoteNode> for RemoteNode {
    fn from(n: network::RemoteNode) -> Self {
        Self {
            id: n.node_id,
            addr: n.addr,
            tag: n.tag,
            node_started_at: n.node_started_at.into_option().map(timestamp_to_datetime),
            attributes: n
                .attributes
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect::<NodeAttributes>()
                .into(),
        }
    }
}

impl From<RemoteNode> for network::RemoteNode {
    fn from(n: RemoteNode) -> Self {
        Self {
            node_id: n.id,
            addr: n.addr,
            tag: n.tag,
            node_started_at: n.node_started_at.as_ref().map(datetime_to_timestamp).into(),
            attributes: n
                .attributes
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            ..Self::default()
        }
    }
}

impl From<&RemoteNode> for network::RemoteNode {
    fn from(n: &RemoteNode) -> Self {
        Self {
            node_id: n.id,
            addr: n.addr.clone(),
            tag: n.tag.clone(),
            node_started_at: n.node_started_at.as_ref().map(datetime_to_timestamp).into(),
            attributes: n
                .attributes
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            ..Self::default()
        }
    }
}

impl From<RemoteNodeState> for network::RemoteNode {
    fn from(s: RemoteNodeState) -> Self {
        Self {
            node_id: s.id,
            addr: s.addr.clone(),
            tag: s.tag.clone(),
            node_started_at: s.node_started_at.as_ref().map(datetime_to_timestamp).into(),
            attributes: s
                .attributes
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            ..Self::default()
        }
    }
}

impl From<&network::NodeIdentity> for RemoteNode {
    fn from(n: &network::NodeIdentity) -> Self {
        RemoteNode {
            id: n.node_id,
            addr: n.addr.clone(),
            tag: n.node_tag.clone(),
            node_started_at: n
                .node_started_at
                .clone()
                .into_option()
                .map(timestamp_to_datetime),
            attributes: n
                .attributes
                .iter()
                .map(|(k, v)| (k.clone().into(), v.clone().into()))
                .collect::<NodeAttributes>()
                .into(),
        }
    }
}

impl Default for RemoteNodeState {
    fn default() -> Self {
        RemoteNodeState {
            id: NodeId::default(),
            addr: String::default(),
            tag: String::default(),
            status: NodeStatus::Joining,
            ping_latency: None,
            last_heartbeat: None,
            node_started_at: None,
            attributes: Arc::new(NodeAttributes::new()),
        }
    }
}

impl RemoteNode {
    pub fn new(
        id: u64,
        addr: String,
        tag: String,
        node_started_at: Option<DateTime<Utc>>,
        attributes: NodeAttributesRef,
    ) -> RemoteNode {
        RemoteNode {
            id,
            addr,
            tag,
            node_started_at,
            attributes,
        }
    }
}

impl Display for RemoteNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", format!("{}|{}", self.addr, self.id))
    }
}

impl PartialEq for RemoteNode {
    fn eq(&self, other: &RemoteNode) -> bool {
        self.id == other.id && self.addr == other.addr
    }
}

impl PartialEq for RemoteNodeState {
    fn eq(&self, other: &RemoteNodeState) -> bool {
        self.id == other.id && self.addr == other.addr
    }
}
