mod cluster;
mod node;
mod routes;

use crate::actor::system::ActorSystem;
use crate::actor::{Actor, ActorFactory, IntoActor, LocalActorRef};
use crate::remote::cluster::sharding::host::ShardHost;
use crate::remote::cluster::sharding::Sharding;
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
        self.into_actor(Some("ShardingApi".to_string()), actor_system)
            .await
            .expect("unable to start ShardingApi actor")
    }
}
