use crate::remote::api::Routes;
use crate::remote::cluster::node::NodeStatus;
use crate::remote::system::{NodeId, RemoteActorSystem};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};

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
        let mut router = router;
        let system = self.system.clone();
        router = router.route("/cluster/nodes", get(move || get_nodes(system)));

        let system = self.system.clone();
        router.route("/node/metrics", get(move || get_metrics(system)))
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
}

#[derive(Serialize, Deserialize)]
pub struct ClusterNodes {
    leader_node: Option<NodeId>,
    leader_node_tag: Option<String>,
    nodes: Vec<ClusterNode>,
}

async fn get_nodes(system: RemoteActorSystem) -> impl IntoResponse {
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
        })
        .collect();

    nodes.sort_by(|a, b| a.id.partial_cmp(&b.id).unwrap());
    let nodes = nodes;

    let leader_node_tag = if leader_node == Some(system.node_id()) {
        Some(system.node_tag().to_string())
    } else {
        if let Some(node) = nodes.iter().filter(|n| Some(n.id) == leader_node).next() {
            Some(node.tag.clone())
        } else {
            None
        }
    };

    Json(ClusterNodes {
        leader_node,
        leader_node_tag,
        nodes,
    })
}

#[derive(Serialize, Deserialize)]
pub struct ActorSystemMetrics {
    pub total_messages_processed: u64,
    pub total_actors_started: u64,
    pub total_actors_stopped: u64,
}

async fn get_metrics(system: RemoteActorSystem) -> impl IntoResponse {
    let metrics = system.actor_system().metrics();

    Json(ActorSystemMetrics {
        total_messages_processed: metrics.get_msgs_processed(),
        total_actors_started: metrics.get_actors_started(),
        total_actors_stopped: metrics.get_actors_stopped(),
    })
}
