use hashring::HashRing;

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use uuid::Uuid;

pub struct RemoteNodeStore<K> {
    nodes: HashMap<Uuid, RemoteNode>,
    table: HashRing<RemoteNode, K>,
}

#[derive(Debug, Copy, Clone)]
pub struct RemoteNode {
    pub id: Uuid,
    pub addr: SocketAddr,
}

impl<K> RemoteNodeStore<K>
where
    K: ToString,
{
    pub fn new(nodes: Vec<RemoteNode>) -> RemoteNodeStore<K> {
        let mut table = HashRing::new();

        let nodes = nodes
            .into_iter()
            .map(|n| {
                table.add(n.clone());
                (n.id.clone(), n)
            })
            .collect();

        RemoteNodeStore { table, nodes }
    }

    pub fn remove(&mut self, node_id: &Uuid) -> Option<RemoteNode> {
        match self.nodes.remove(&node_id) {
            Some(node) => self.table.remove(node),
            None => None,
        }
    }

    pub fn get_by_key(&mut self, key: K) -> Option<&RemoteNode> {
        self.table.get(key)
    }

    pub fn add(&mut self, node: RemoteNode) {
        self.table.add(node.clone());
        self.nodes.insert(node.id.clone(), node);
    }
}

impl RemoteNode {
    pub fn new(id: Uuid, ip: &str, port: u16) -> RemoteNode {
        let addr = SocketAddr::new(IpAddr::from_str(&ip).unwrap(), port);
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
