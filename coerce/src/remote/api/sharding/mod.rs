pub mod cluster;
pub mod node;
pub mod routes;

use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message};
use crate::actor::system::ActorSystem;
use crate::actor::{Actor, ActorFactory, IntoActor, IntoActorId, LocalActorRef};
use crate::sharding::host::ShardHost;
use crate::sharding::Sharding;
use axum::response::IntoResponse;
use axum::Json;
use std::collections::HashMap;

#[derive(Default)]
pub struct ShardingApi {
    shard_hosts: HashMap<String, LocalActorRef<ShardHost>>,
}

impl Actor for ShardingApi {}

impl ShardingApi {
    pub fn attach<F: ActorFactory>(mut self, sharding: &Sharding<F>) -> Self {
        let shard_entity = sharding.shard_entity().clone();
        let shard_host = sharding.shard_host().clone();

        self.shard_hosts.insert(shard_entity, shard_host);
        self
    }

    pub async fn start(self, actor_system: &ActorSystem) -> LocalActorRef<ShardingApi> {
        self.into_actor(Some("sharding-api".into_actor_id()), actor_system)
            .await
            .expect("unable to start ShardingApi actor")
    }
}

pub struct GetShardTypes;

impl Message for GetShardTypes {
    type Result = ShardTypes;
}

#[async_trait]
impl Handler<GetShardTypes> for ShardingApi {
    async fn handle(&mut self, _message: GetShardTypes, _ctx: &mut ActorContext) -> ShardTypes {
        ShardTypes {
            entities: self.shard_hosts.keys().cloned().collect(),
        }
    }
}

#[derive(Serialize, ToSchema)]
pub struct ShardTypes {
    entities: Vec<String>,
}

#[utoipa::path(
    get,
    path = "/sharding/types",
    responses(
        (status = 200, description = "All sharded entity types", body = ShardTypes),
    ),
)]
pub async fn get_sharding_types(sharding_api: LocalActorRef<ShardingApi>) -> impl IntoResponse {
    Json(
        sharding_api
            .send(GetShardTypes)
            .await
            .expect("unable to get shard types"),
    )
}
