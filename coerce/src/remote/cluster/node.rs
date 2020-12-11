use hashring::HashRing;

use std::collections::HashMap;

use uuid::Uuid;

pub struct RemoteNodeStore {
    nodes: HashMap<Uuid, RemoteNode>,
    table: HashRing<RemoteNode, String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RemoteNode {
    pub id: Uuid,
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

    pub fn get(&self, node_id: &Uuid) -> Option<&RemoteNode> {
        self.nodes.get(node_id)
    }

    pub fn is_registered(&self, node_id: Uuid) -> bool {
        self.nodes.contains_key(&node_id)
    }

    pub fn remove(&mut self, node_id: &Uuid) -> Option<RemoteNode> {
        self.nodes
            .remove(&node_id)
            .and_then(|node| self.table.remove(node))
    }

    pub fn get_by_key(&mut self, key: impl ToString) -> Option<&RemoteNode> {
        self.table.get(key.to_string())
    }

    pub fn add(&mut self, node: RemoteNode) {
        self.table.add(node.clone());
        self.nodes.insert(node.id.clone(), node);
    }

    pub fn get_all(&self) -> Vec<RemoteNode> {
        self.nodes.values().map(|m| m.clone()).collect()
    }
}

impl RemoteNode {
    pub fn new(id: Uuid, addr: String) -> RemoteNode {
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
