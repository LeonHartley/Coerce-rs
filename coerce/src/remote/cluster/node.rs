use crate::remote::system::NodeId;

use hashring::HashRing;

use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::hash::Hash;
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

#[derive(Debug, Clone)]
pub struct RemoteNodeState {
    pub id: NodeId,
    pub addr: String,
    pub tag: String,
    pub ping_latency: Option<Duration>,
    pub last_heartbeat: Option<DateTime<Utc>>,
    pub node_started_at: Option<DateTime<Utc>>,
    pub status: NodeStatus,
}

#[derive(Hash, Debug, Clone)]
pub struct RemoteNode {
    pub id: NodeId,
    pub addr: String,
    pub tag: String,
    pub node_started_at: Option<DateTime<Utc>>,
}

#[derive(Clone)]
pub struct NodeIdentity {
    pub node: RemoteNode,
    pub peers: Vec<RemoteNode>,
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

    pub fn get(&self, node_id: &NodeId) -> Option<&RemoteNodeState> {
        self.nodes.get(node_id)
    }

    pub fn is_registered(&self, node_id: NodeId) -> bool {
        self.nodes.contains_key(&node_id)
    }

    pub fn remove(&mut self, node_id: &NodeId) -> Option<RemoteNode> {
        self.nodes.remove(&node_id).and_then(|node| {
            self.table
                .remove(&RemoteNode::new(node.id, node.addr, node.tag, None))
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
                self.table.add(RemoteNode::new(
                    n.id,
                    n.addr,
                    node.tag.clone(),
                    node.node_started_at,
                ));
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
        }
    }
}

impl RemoteNode {
    pub fn new(
        id: u64,
        addr: String,
        tag: String,
        node_started_at: Option<DateTime<Utc>>,
    ) -> RemoteNode {
        RemoteNode {
            id,
            addr,
            tag,
            node_started_at,
        }
    }
}

impl ToString for RemoteNode {
    fn to_string(&self) -> String {
        format!("{}|{}", self.addr, self.id)
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
