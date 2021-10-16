use crate::remote::system::NodeId;

use hashring::HashRing;

use std::collections::HashMap;
use std::hash::Hash;

pub struct RemoteNodeStore {
    nodes: HashMap<NodeId, RemoteNode>,
    table: HashRing<RemoteNode>,
}

#[derive(Hash, Serialize, Deserialize, Debug, Clone)]
pub struct RemoteNode {
    pub id: NodeId,
    pub addr: String,
}

impl RemoteNodeStore {
    pub fn new(nodes: Vec<RemoteNode>) -> RemoteNodeStore {
        let mut table = HashRing::new();

        let nodes = nodes
            .into_iter()
            .map(|n| {
                table.add(n.clone());
                (n.id, n)
            })
            .collect();

        RemoteNodeStore { table, nodes }
    }

    pub fn get(&self, node_id: &NodeId) -> Option<&RemoteNode> {
        self.nodes.get(node_id)
    }

    pub fn is_registered(&self, node_id: NodeId) -> bool {
        self.nodes.contains_key(&node_id)
    }

    pub fn remove(&mut self, node_id: &NodeId) -> Option<RemoteNode> {
        self.nodes
            .remove(&node_id)
            .and_then(|node| self.table.remove(&node))
    }

    pub fn get_by_key(&mut self, key: impl Hash) -> Option<&RemoteNode> {
        self.table.get(&key)
    }

    pub fn add(&mut self, node: RemoteNode) {
        let mut nodes = self.get_all();
        nodes.push(node);
        nodes.sort_by(|a, b| a.id.partial_cmp(&b.id).unwrap());

        self.table = HashRing::new();
        self.nodes = nodes
            .into_iter()
            .map(|n| {
                self.table.add(n.clone());
                (n.id, n)
            })
            .collect();
    }

    pub fn get_all(&self) -> Vec<RemoteNode> {
        self.nodes.values().cloned().collect()
    }
}

impl RemoteNode {
    pub fn new(id: u64, addr: String) -> RemoteNode {
        RemoteNode { id, addr }
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
