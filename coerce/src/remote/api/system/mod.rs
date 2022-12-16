pub mod actors;

use crate::remote::api::Routes;
use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::actor::scheduler::ActorCount;
use crate::remote::system::{NodeId, RemoteActorSystem};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use futures::future::join_all;
use tokio::sync::oneshot;

use crate::actor::context::ActorStatus;
use crate::actor::lifecycle::Status;
use crate::actor::{ActorPath, BoxedActorRef, CoreActorRef, IntoActorPath};
use crate::remote::api::cluster::ClusterNode;
use crate::remote::api::openapi::ApiDoc;
use crate::remote::heartbeat::{health, Heartbeat};
use crate::remote::stream::pubsub::PubSub;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

pub struct SystemApi {
    system: RemoteActorSystem,
}

impl SystemApi {
    pub fn new(system: RemoteActorSystem) -> Self {
        Self { system }
    }
}

impl Routes for SystemApi {
    fn routes(&self, router: Router) -> Router {
        router
            .merge(SwaggerUi::new("/swagger").url("/api-doc/openapi.json", ApiDoc::openapi()))
            .route("/health", {
                let system = self.system.clone();
                get(move || health(system))
            })
            .route("/system/stats", {
                let system = self.system.clone();
                get(move || get_stats(system))
            })
            .route("/actors/all", {
                let system = self.system.clone();
                get(move |options| actors::get_all(system, options))
            })
    }
}

#[derive(Serialize, ToSchema, Eq, PartialEq, Clone, Copy)]
pub enum HealthStatus {
    Healthy,
    Degraded,
    Unhealthy,
}

#[derive(Serialize, ToSchema)]
pub struct SystemHealth {
    status: HealthStatus,
    node_id: u64,
    node_tag: String,
    node_version: String,
    node_started_at: DateTime<Utc>,
    runtime_version: &'static str,
    actor_response_times: HashMap<ActorPath, Option<Duration>>,
    current_leader: Option<NodeId>,
    nodes: Vec<ClusterNode>,
}

#[utoipa::path(
    get,
    path = "/health",
    responses(
        (status = 200, description = "System Health", body = SystemHealth),
    )
)]
async fn health(system: RemoteActorSystem) -> Json<SystemHealth> {
    Json(Heartbeat::get_system_health(&system).await.into())
}

impl From<health::SystemHealth> for SystemHealth {
    fn from(value: health::SystemHealth) -> Self {
        Self {
            status: value.status.into(),
            node_id: value.node_id,
            node_tag: value.node_tag,
            node_version: value.node_version,
            node_started_at: value.node_started_at,
            runtime_version: value.runtime_version,
            actor_response_times: value.actor_response_times,
            current_leader: value.current_leader,
            nodes: value.nodes.into_iter().map(|n| n.into()).collect(),
        }
    }
}

impl From<health::HealthStatus> for HealthStatus {
    fn from(value: health::HealthStatus) -> Self {
        match value {
            health::HealthStatus::Healthy => Self::Healthy,
            health::HealthStatus::Degraded => Self::Degraded,
            health::HealthStatus::Unhealthy => Self::Unhealthy,
        }
    }
}

#[derive(Serialize, Deserialize, ToSchema)]
pub struct SystemStats {
    inflight_remote_requests: usize,
    total_tracked_actors: usize,
    remote_actor_ref_cache_len: usize,
}

#[utoipa::path(
    get,
    path = "/system/stats",
    responses(
        (status = 200, description = "System Statistics", body = SystemStats),
    )
)]
async fn get_stats(system: RemoteActorSystem) -> impl IntoResponse {
    use crate::remote::handler::actor_ref_cache_size;

    Json(SystemStats {
        inflight_remote_requests: system.inflight_remote_request_count(),
        total_tracked_actors: system
            .actor_system()
            .scheduler()
            .send(ActorCount)
            .await
            .unwrap(),
        remote_actor_ref_cache_len: actor_ref_cache_size(),
    })
}
