use crate::remote::api::Routes;
use crate::remote::cluster::node::RemoteNodeState;
use crate::remote::system::RemoteActorSystem;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use std::collections::HashMap;
use std::time::Duration;

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

#[derive(Serialize, Deserialize, Debug, ToSchema)]
pub struct ClusterNode {
    pub id: u64,
    pub addr: String,
    pub tag: String,
    pub ping_latency: Option<Duration>,
    pub last_heartbeat: Option<String>,
    pub node_started_at: Option<String>,
    pub status: NodeStatus,
    pub attributes: HashMap<String, String>,
}

#[derive(Serialize, Deserialize, Debug, ToSchema)]
pub struct ClusterNodes {
    pub node_id: u64,
    pub leader_node: Option<u64>,
    pub leader_node_tag: Option<String>,
    pub nodes: Vec<ClusterNode>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub enum NodeStatus {
    Joining,
    Healthy,
    Unhealthy,
    Terminated,
}

impl From<crate::remote::cluster::node::NodeStatus> for NodeStatus {
    fn from(value: crate::remote::cluster::node::NodeStatus) -> Self {
        match value {
            crate::remote::cluster::node::NodeStatus::Joining => Self::Joining,
            crate::remote::cluster::node::NodeStatus::Healthy => Self::Healthy,
            crate::remote::cluster::node::NodeStatus::Unhealthy => Self::Unhealthy,
            crate::remote::cluster::node::NodeStatus::Terminated => Self::Terminated,
        }
    }
}

#[utoipa::path(
    get,
    path = "/cluster/nodes",
    responses(
    (
        status = 200, description = "All known Coerce cluster nodes", body = ClusterNodes),
    )
)]
async fn get_nodes(system: RemoteActorSystem) -> impl IntoResponse {
    let node_id = system.node_id();
    let leader_node = system.current_leader();
    let mut nodes: Vec<ClusterNode> = system
        .get_nodes()
        .await
        .into_iter()
        .map(|node| node.into())
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

impl From<RemoteNodeState> for ClusterNode {
    fn from(node: RemoteNodeState) -> Self {
        ClusterNode {
            id: node.id,
            addr: node.addr,
            tag: node.tag,
            ping_latency: node.ping_latency,
            last_heartbeat: node.last_heartbeat.map(|h| format!("{:?}", h)),
            node_started_at: node.node_started_at.map(|p| format!("{:?}", p)),
            status: node.status.into(),
            attributes: node
                .attributes
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }
}
