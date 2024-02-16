//! Coerce Distributed Sharding

use crate::actor::message::{Handler, Message};
use crate::actor::{
    Actor, ActorFactory, ActorId, ActorRecipe, ActorRefErr, IntoActor, IntoActorId, LocalActorRef,
};
use std::error::Error;
use std::fmt::{Display, Formatter};

use crate::remote::system::builder::RemoteSystemConfigBuilder;
use crate::remote::system::RemoteActorSystem;
use crate::sharding::coordinator::allocation::AllocateShard;
use crate::sharding::coordinator::factory::CoordinatorFactory;
use crate::sharding::coordinator::stats::GetShardingStats;
use crate::sharding::coordinator::ShardCoordinator;
use crate::sharding::host::request::{EntityRequest, RemoteEntityRequest};
use crate::sharding::host::{
    Init, ShardAllocated, ShardAllocator, ShardHost, ShardReallocating, StopShard,
};
use crate::sharding::shard::stats::GetShardStats;
use crate::sharding::shard::Shard;
use crate::singleton::{singleton, Singleton, SingletonBuilder};
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
    coordinator: Singleton<ShardCoordinator, CoordinatorFactory>,
    shard_entity: String,
    system: RemoteActorSystem,
}

pub struct Sharded<A: Actor> {
    sharding: Arc<ShardingCore>,
    actor_id: ActorId,
    recipe: Option<Arc<Vec<u8>>>,
    _a: PhantomData<A>,
}

#[derive(Debug, Clone)]
pub enum StartupErr {
    MissingActorFactory {
        entity_name: String,
        entity_type: &'static str,
    },
    CoordinatorErr {
        entity_name: String,
        err: ActorRefErr,
    },
    ShardHostErr {
        entity_name: String,
        err: ActorRefErr,
    },
}

impl<A: ActorFactory> Sharding<A> {
    pub async fn try_start(
        shard_entity: String,
        system: RemoteActorSystem,
        allocator: Option<Box<dyn ShardAllocator>>,
    ) -> Result<Self, StartupErr> {
        let actor_type = A::Actor::type_name();
        let actor_handler = system.config().actor_handler(actor_type).ok_or_else(|| {
            StartupErr::MissingActorFactory {
                entity_name: shard_entity.clone(),
                entity_type: actor_type,
            }
        })?;

        let host = ShardHost::new(shard_entity.clone(), actor_handler, allocator)
            .into_actor(
                Some(ShardHost::actor_id(&shard_entity, system.node_id())),
                system.actor_system(),
            )
            .await
            .map_err(|e| e.into_host_err(&shard_entity))?;

        let coordinator = SingletonBuilder::new(system.clone())
            .factory(CoordinatorFactory::new(shard_entity.clone(), host.clone()))
            .build()
            .await;

        let _ = host.send(Init(coordinator.clone())).await;

        Ok(Self {
            core: Arc::new(ShardingCore {
                host,
                system,
                coordinator,
                shard_entity,
            }),
            _a: PhantomData,
        })
    }

    pub async fn start(
        shard_entity: String,
        system: RemoteActorSystem,
        allocator: Option<Box<dyn ShardAllocator>>,
    ) -> Self {
        Self::try_start(shard_entity, system, allocator)
            .await
            .expect("start sharding")
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

        let message_type = self.sharding.system.handler_name::<A, M>().ok_or_else(|| {
            ActorRefErr::NotSupported {
                actor_id: self.actor_id.clone(),
                message_type: M::type_name().to_string(),
                actor_type: A::type_name().to_string(),
            }
        })?;

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

            result.unwrap_or_else(|e| Err(e))
        } else {
            Err(ActorRefErr::ResultChannelClosed)
        }
    }
}

pub fn sharding(builder: &mut RemoteSystemConfigBuilder) -> &mut RemoteSystemConfigBuilder {
    singleton::<CoordinatorFactory>(builder)
        .with_handler::<ShardCoordinator, AllocateShard>("ShardCoordinator.AllocateShard")
        .with_handler::<ShardCoordinator, GetShardingStats>("ShardCoordinator.GetShardingStats")
        .with_handler::<ShardHost, ShardAllocated>("ShardHost.ShardAllocated")
        .with_handler::<ShardHost, ShardReallocating>("ShardHost.ShardReallocating")
        .with_handler::<ShardHost, StopShard>("ShardHost.StopShard")
        .with_handler::<Shard, RemoteEntityRequest>("Shard.RemoteEntityRequest")
        .with_handler::<Shard, GetShardStats>("Shard.GetShardStats")
}

impl Error for StartupErr {}

impl Display for StartupErr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self {
            StartupErr::MissingActorFactory { entity_name, entity_type } => write!(f, "failed to initialise sharding for entity={}, factory not found for type={}, please register it via RemoteActorSystemBuilder", entity_name, entity_type),
            StartupErr::ShardHostErr { entity_name, err,  } => write!(f, "failed to start ShardHost for entity={}, error={}", entity_name, err),
            StartupErr::CoordinatorErr { entity_name, err } => write!(f, "failed to start ShardCoordinatotor spawner for entity={}, error={}", entity_name, err),
        }
    }
}

impl ActorRefErr {
    pub fn into_host_err(self, entity: &str) -> StartupErr {
        StartupErr::ShardHostErr {
            entity_name: entity.to_string(),
            err: self,
        }
    }

    pub fn into_coordinator_err(self, entity: &str) -> StartupErr {
        StartupErr::CoordinatorErr {
            entity_name: entity.to_string(),
            err: self,
        }
    }
}
