pub mod actors;

use crate::remote::api::Routes;

use crate::actor::scheduler::ActorCount;
use crate::remote::system::RemoteActorSystem;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};

use crate::remote::api::openapi::ApiDoc;
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

#[derive(Serialize, Deserialize, ToSchema)]
pub struct SystemStats {
    inflight_remote_requests: usize,
    total_tracked_actors: usize,
}

#[utoipa::path(
    get,
    path = "/system/stats",
    responses(
        (status = 200, description = "System statistics", body = SystemStats),
    )
)]
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
