use crate::actor::LocalActorRef;
use crate::sharding::coordinator::ShardCoordinator;
use crate::sharding::host::ShardHost;
use crate::singleton::factory::SingletonFactory;

pub struct CoordinatorFactory {
    shard_entity: String,
    local_shard_host: LocalActorRef<ShardHost>,
}

impl CoordinatorFactory {
    pub fn new(shard_entity: String, local_shard_host: LocalActorRef<ShardHost>) -> Self {
        CoordinatorFactory {
            shard_entity,
            local_shard_host,
        }
    }
}

impl SingletonFactory for CoordinatorFactory {
    type Actor = ShardCoordinator;

    fn create(&self) -> Self::Actor {
        ShardCoordinator::new(self.shard_entity.clone(), self.local_shard_host.clone())
    }
}
