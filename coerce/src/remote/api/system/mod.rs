pub mod actors;

use crate::remote::api::Routes;

use crate::actor::scheduler::ActorCount;
use crate::remote::api::system::actors::describe_all;
use crate::remote::system::RemoteActorSystem;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};

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
            .route("/system/stats", {
                let system = self.system.clone();
                get(move || get_stats(system))
            })
            .route("/system/describe/all", {
                let system = self.system.clone();
                get(move |options| describe_all(system, options))
            })
    }
}

#[derive(Serialize, Deserialize)]
pub struct SystemStats {
    inflight_remote_requests: usize,
    total_tracked_actors: usize,
}

async fn get_stats(system: RemoteActorSystem) -> impl IntoResponse {
    Json(SystemStats {
        inflight_remote_requests: system.inflight_remote_request_count(),
        total_tracked_actors: system
            .actor_system()
            .scheduler()
            .send(ActorCount)
            .await
            .unwrap(),
    })
}
