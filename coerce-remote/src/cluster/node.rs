use hashring::HashRing;

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
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

    pub fn get_by_key<K: ToString>(&mut self, key: K) -> Option<&RemoteNode> {
        self.table.get(key.to_string())
    }

    pub fn add(&mut self, node: RemoteNode) {
        self.table.add(node.clone());
        self.nodes.insert(node.id.clone(), node);
    }
}

impl RemoteNode {
    pub fn new(id: Uuid, ip: &str, port: u16) -> RemoteNode {
        let addr = SocketAddr::new(IpAddr::from_str(&ip).unwrap(), port);
        RemoteNode {
            id,
            addr: addr.to_string(),
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
