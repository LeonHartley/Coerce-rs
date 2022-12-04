use crate::remote::api::Routes;
use crate::remote::cluster::node::{NodeAttributesRef, NodeStatus};
use crate::remote::system::{NodeId, RemoteActorSystem};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use std::collections::HashMap;

pub struct ClusterApi {
    system: RemoteActorSystem,
}

impl ClusterApi {
    pub fn new(system: RemoteActorSystem) -> Self {
        Self { system }
    }
}

impl Routes for ClusterApi {
    fn routes(&self, router: Router) -> Router {
        router.route("/cluster/nodes", {
            let system = self.system.clone();
            get(move || get_nodes(system))
        })
    }
}

#[derive(Serialize, Deserialize)]
pub struct ClusterNode {
    pub id: NodeId,
    pub addr: String,
    pub tag: String,
    pub ping_latency: Option<String>,
    pub last_heartbeat: Option<String>,
    pub node_started_at: Option<String>,
    pub status: NodeStatus,
    pub attributes: NodeAttributesRef,
}

#[derive(Serialize, Deserialize)]
pub struct ClusterNodes {
    node_id: NodeId,
    leader_node: Option<NodeId>,
    leader_node_tag: Option<String>,
    nodes: Vec<ClusterNode>,
}

async fn get_nodes(system: RemoteActorSystem) -> impl IntoResponse {
    let node_id = system.node_id();
    let leader_node = system.current_leader();
    let mut nodes: Vec<ClusterNode> = system
        .get_nodes()
        .await
        .into_iter()
        .map(|node| ClusterNode {
            id: node.id,
            addr: node.addr,
            tag: node.tag,
            ping_latency: node.ping_latency.map(|p| format!("{:?}", p)),
            last_heartbeat: node.last_heartbeat.map(|h| format!("{:?}", h)),
            node_started_at: node.node_started_at.map(|p| format!("{:?}", p)),
            status: node.status,
            attributes: node.attributes.clone(),
        })
        .collect();

    nodes.sort_by(|a, b| a.id.partial_cmp(&b.id).unwrap());
    let nodes = nodes;

    let leader_node_tag = if leader_node == Some(system.node_id()) {
        Some(system.node_tag().to_string())
    } else {
        nodes
            .iter()
            .find(|n| Some(n.id) == leader_node)
            .map(|node| node.tag.clone())
    };

    Json(ClusterNodes {
        node_id,
        leader_node,
        leader_node_tag,
        nodes,
    })
}
