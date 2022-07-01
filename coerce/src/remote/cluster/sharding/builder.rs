use crate::actor::{Actor, ActorFactory};
use crate::remote::cluster::sharding::host::ShardAllocator;
use crate::remote::cluster::sharding::Sharding;
use crate::remote::system::RemoteActorSystem;
use std::marker::PhantomData;

pub struct ShardingBuilder<A: ActorFactory> {
    shard_allocator: Option<Box<dyn ShardAllocator>>,
    shard_entity: Option<String>,
    system: Option<RemoteActorSystem>,
    _a: PhantomData<A>,
}

impl<A: ActorFactory> Sharding<A> {
    pub fn builder(system: RemoteActorSystem) -> ShardingBuilder<A> {
        ShardingBuilder {
            shard_allocator: None,
            shard_entity: None,
            system: Some(system),
            _a: PhantomData,
        }
    }
}

impl<A: ActorFactory> ShardingBuilder<A> {
    pub fn with_entity_type<S: ToString>(&mut self, entity_type: S) -> &mut Self {
        self.shard_entity = Some(entity_type.to_string());
        self
    }

    pub fn with_allocator<S: ShardAllocator>(&mut self, allocator: S) -> &mut Self {
        self.shard_allocator = Some(Box::new(allocator));
        self
    }

    pub async fn build(&mut self) -> Sharding<A> {
        Sharding::start(
            self.shard_entity
                .take()
                .unwrap_or_else(|| A::Actor::type_name().to_string()),
            self.system.take().unwrap(),
            self.shard_allocator.take(),
        )
        .await
    }
}
