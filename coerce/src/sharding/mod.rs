use crate::actor::message::{Handler, Message};
use crate::actor::{
    Actor, ActorFactory, ActorId, ActorRecipe, ActorRefErr, IntoActor, IntoActorId, LocalActorRef,
};

use crate::remote::system::builder::RemoteSystemConfigBuilder;
use crate::remote::system::RemoteActorSystem;
use crate::sharding::coordinator::allocation::AllocateShard;
use crate::sharding::coordinator::spawner::CoordinatorSpawner;
use crate::sharding::coordinator::stats::GetShardingStats;
use crate::sharding::coordinator::ShardCoordinator;
use crate::sharding::host::request::{EntityRequest, RemoteEntityRequest};
use crate::sharding::host::{
    ShardAllocated, ShardAllocator, ShardHost, ShardReallocating, StopShard,
};
use crate::sharding::shard::stats::GetShardStats;
use crate::sharding::shard::Shard;
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::sync::oneshot;

pub mod builder;
pub mod coordinator;
pub mod host;
pub mod proto;
pub mod shard;

#[derive(Clone)]
pub struct Sharding<A: ActorFactory> {
    core: Arc<ShardingCore>,
    _a: PhantomData<A>,
}

struct ShardingCore {
    host: LocalActorRef<ShardHost>,
    coordinator_spawner: LocalActorRef<CoordinatorSpawner>,
    shard_entity: String,
    system: RemoteActorSystem,
}

pub struct Sharded<A: Actor> {
    sharding: Arc<ShardingCore>,
    actor_id: ActorId,
    recipe: Option<Arc<Vec<u8>>>,
    _a: PhantomData<A>,
}

impl<A: ActorFactory> Sharding<A> {
    pub async fn start(
        shard_entity: String,
        system: RemoteActorSystem,
        allocator: Option<Box<dyn ShardAllocator>>,
    ) -> Self {
        let coordinator_spawner_actor_id = Some(
            format!(
                "ShardCoordinator-{}-Spawner-{}",
                &shard_entity,
                system.node_id()
            )
            .into_actor_id(),
        );

        let host_actor_id =
            Some(format!("ShardHost-{}-{}", &shard_entity, system.node_id()).into_actor_id());

        let actor_handler = match system
            .config()
            .actor_handler(A::Actor::type_name()) {
            None => panic!("failed to initialise sharding for entity={}, factory not found for type={}, please register it via RemoteActorSystemBuilder", &shard_entity, A::Actor::type_name()),
            Some(handler) => handler,
        };

        let host = ShardHost::new(shard_entity.clone(), actor_handler, allocator)
            .into_actor(host_actor_id, system.actor_system())
            .await
            .expect("create ShardHost actor");

        let coordinator_spawner =
            CoordinatorSpawner::new(system.node_id(), shard_entity.clone(), host.clone())
                .into_actor(coordinator_spawner_actor_id, system.actor_system())
                .await
                .expect("create ShardCoordinator spawner");

        Self {
            core: Arc::new(ShardingCore {
                host,
                system,
                coordinator_spawner,
                shard_entity,
            }),
            _a: PhantomData,
        }
    }

    pub fn get(&self, actor_id: impl IntoActorId, recipe: Option<A::Recipe>) -> Sharded<A::Actor> {
        let actor_id = actor_id.into_actor_id();
        let recipe = match recipe {
            Some(recipe) => recipe.write_to_bytes().map(Arc::new),
            None => None,
        };

        let sharding = self.core.clone();
        Sharded {
            actor_id,
            recipe,
            sharding,
            _a: Default::default(),
        }
    }

    pub fn system(&self) -> &RemoteActorSystem {
        &self.core.system
    }

    pub fn notify_host<M: Message>(&self, message: M) -> Result<(), ActorRefErr>
    where
        ShardHost: Handler<M>,
    {
        self.core.notify_host(message)
    }

    pub fn shard_host(&self) -> &LocalActorRef<ShardHost> {
        &self.core.host
    }

    pub fn shard_entity(&self) -> &String {
        &self.core.shard_entity
    }

    pub fn coordinator_spawner(&self) -> &LocalActorRef<CoordinatorSpawner> {
        &self.core.coordinator_spawner
    }
}

impl ShardingCore {
    pub fn notify_host<M: Message>(&self, message: M) -> Result<(), ActorRefErr>
    where
        ShardHost: Handler<M>,
    {
        self.host.notify(message)
    }
}

impl<A: Actor> Sharded<A> {
    pub async fn send<M: Message>(&self, message: M) -> Result<M::Result, ActorRefErr>
    where
        A: Handler<M>,
    {
        let message = match message.as_bytes() {
            Ok(bytes) => bytes,
            Err(e) => return Err(ActorRefErr::Serialisation(e)),
        };

        let message_type = self
            .sharding
            .system
            .config()
            .handler_name::<A, M>()
            .expect("message not setup for remoting");

        let (tx, rx) = oneshot::channel();

        let actor_id = self.actor_id.clone();
        let _res = self.sharding.notify_host(EntityRequest {
            actor_id,
            message_type,
            message,
            recipe: self.recipe.clone(),
            result_channel: Some(tx),
        });

        let result = rx.await;
        if let Ok(result) = result {
            let result =
                result.map(|res| M::read_remote_result(res).map_err(ActorRefErr::Deserialisation));
            match result {
                Ok(res) => res,
                Err(e) => Err(e),
            }
        } else {
            Err(ActorRefErr::ResultChannelClosed)
        }
    }
}

pub fn sharding(builder: &mut RemoteSystemConfigBuilder) -> &mut RemoteSystemConfigBuilder {
    builder
        .with_handler::<ShardCoordinator, AllocateShard>("ShardCoordinator.AllocateShard")
        .with_handler::<ShardCoordinator, GetShardingStats>("ShardCoordinator.GetShardingStats")
        .with_handler::<ShardHost, ShardAllocated>("ShardHost.ShardAllocated")
        .with_handler::<ShardHost, ShardReallocating>("ShardHost.ShardReallocating")
        .with_handler::<ShardHost, StopShard>("ShardHost.StopShard")
        .with_handler::<Shard, RemoteEntityRequest>("Shard.RemoteEntityRequest")
        .with_handler::<Shard, GetShardStats>("Shard.GetShardStats")
}
