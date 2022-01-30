use crate::actor::LocalActorRef;
use crate::remote::api::sharding::cluster::get_sharding_stats;
use crate::remote::api::sharding::node::{GetAllStats, GetShardTypes, GetStats};
use crate::remote::api::sharding::ShardingApi;
use crate::remote::api::Routes;
use axum::extract::Path;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};

impl Routes for LocalActorRef<ShardingApi> {
    fn routes(&self, mut router: Router) -> Router {
        let actor_ref = self.clone();
        router = router.route(
            "/sharding",
            get(|| async move {
                let actor_ref = actor_ref;
                Json(
                    actor_ref
                        .send(GetShardTypes)
                        .await
                        .expect("unable to get shard types"),
                )
            }),
        );

        let actor_ref = self.clone();
        router = router.route(
            "/sharding/cluster/stats/:entity",
            get(move |path| get_sharding_stats(actor_ref, path)),
        );

        let actor_ref = self.clone();
        router = router.route(
            "/sharding/node/stats/:entity",
            get({
                async fn get_stats(
                    Path(entity): Path<String>,
                    actor_ref: LocalActorRef<ShardingApi>,
                ) -> impl IntoResponse {
                    Json(
                        actor_ref
                            .send(GetStats(entity))
                            .await
                            .expect("unable to get stats"),
                    )
                }

                let actor_ref = actor_ref;
                move |path| get_stats(path, actor_ref)
            }),
        );

        let actor_ref = self.clone();
        router = router.route(
            "/sharding/node/stats/all",
            get(|| async move {
                let actor_ref = actor_ref;
                Json(
                    actor_ref
                        .send(GetAllStats)
                        .await
                        .expect("unable to get shard types"),
                )
            }),
        );

        router
    }
}
