use crate::actor::LocalActorRef;
use crate::remote::api::sharding::cluster::{get_shard_host_stats, get_sharding_stats};
use crate::remote::api::sharding::node::{get_node_stats, GetAllStats};
use crate::remote::api::sharding::{get_sharding_types, ShardingApi};
use crate::remote::api::Routes;

use axum::routing::get;
use axum::{Json, Router};

impl Routes for LocalActorRef<ShardingApi> {
    fn routes(&self, router: Router) -> Router {
        router
            .route("/sharding/types", {
                let actor_ref = self.clone();
                get(move || get_sharding_types(actor_ref))
            })
            .route("/sharding/stats/cluster/:entity", {
                let actor_ref = self.clone();
                get(move |path| get_sharding_stats(actor_ref, path))
            })
            .route("/sharding/host/stats/:entity", {
                let actor_ref = self.clone();
                get(move |path| get_shard_host_stats(actor_ref, path))
            })
            .route("/sharding/stats/node/:entity", {
                let actor_ref = self.clone();
                get(move |path| get_node_stats(actor_ref, path))
            })
            .route("/sharding/node/stats/all", {
                let actor_ref = self.clone();
                get(|| async move {
                    let actor_ref = actor_ref;
                    Json(
                        actor_ref
                            .send(GetAllStats)
                            .await
                            .expect("unable to get shard types"),
                    )
                })
            })
    }
}
