mod cluster;
mod node;
mod routes;

use crate::actor::context::ActorContext;
use crate::actor::system::ActorSystem;
use crate::actor::{Actor, ActorFactory, IntoActor, LocalActorRef};
use crate::remote::api::Routes;
use crate::remote::cluster::sharding::host::ShardHost;
use crate::remote::cluster::sharding::Sharding;
use std::collections::HashMap;

pub struct ShardingApi {
    shard_hosts: HashMap<String, LocalActorRef<ShardHost>>,
}

impl Actor for ShardingApi {}

impl ShardingApi {
    pub fn new() -> Self {
        ShardingApi {
            shard_hosts: HashMap::new(),
        }
    }

    pub fn attach<F: ActorFactory>(mut self, sharding: &Sharding<F>) -> Self {
        let shard_entity = sharding.shard_entity().clone();
        let shard_host = sharding.shard_host().clone();

        self.shard_hosts.insert(shard_entity, shard_host);
        self
    }

    pub async fn start(mut self, actor_system: &ActorSystem) -> LocalActorRef<ShardingApi> {
        self.into_actor(Some("ShardingApi".to_string()), actor_system)
            .await
            .expect("unable to start ShardingApi actor")
    }
}
