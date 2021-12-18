use crate::actor::message::{Envelope, Handler, Message, MessageUnwrapErr};
use crate::actor::{
    Actor, ActorFactory, ActorId, ActorRecipe, ActorRefErr, IntoActor, LocalActorRef,
};
use crate::remote::cluster::sharding::coordinator::allocation::AllocateShard;
use crate::remote::cluster::sharding::coordinator::spawner::CoordinatorSpawner;
use crate::remote::cluster::sharding::coordinator::ShardCoordinator;
use crate::remote::cluster::sharding::host::request::{EntityRequest, RemoteEntityRequest};
use crate::remote::cluster::sharding::host::{ShardAllocated, ShardHost};
use crate::remote::cluster::sharding::shard::Shard;
use crate::remote::system::builder::RemoteActorHandlerBuilder;
use crate::remote::system::RemoteActorSystem;
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::sync::oneshot;

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
    system: RemoteActorSystem,
}

pub struct Sharded<A: Actor> {
    sharding: Arc<ShardingCore>,
    actor_id: ActorId,
    recipe: Option<Arc<Vec<u8>>>,
    _a: PhantomData<A>,
}

impl<A: ActorFactory> Sharding<A> {
    pub async fn start(system: RemoteActorSystem) -> Self {
        let shard_entity = A::Actor::type_name().to_string();
        let coordinator_spawner_actor_id = Some(format!(
            "ShardCoordinator-{}-Spawner-{}",
            &shard_entity,
            system.node_id()
        ));
        let host_actor_id = Some(format!("ShardHost-{}-{}", &shard_entity, system.node_id()));

        let host = ShardHost::new(shard_entity.clone())
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
            }),
            _a: PhantomData,
        }
    }

    pub fn get(&self, actor_id: ActorId, recipe: Option<A::Recipe>) -> Sharded<A::Actor> {
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
        self.core.host.notify(message)
    }
}

impl<A: Actor> Sharded<A> {
    pub async fn send<M: Message>(&self, message: M) -> Result<M::Result, ActorRefErr>
    where
        A: Handler<M>,
    {
        let message = match message.as_remote_envelope() {
            Ok(Envelope::Remote(b)) => b,
            _ => return Err(ActorRefErr::ActorUnavailable),
        };

        let message_type = self
            .sharding
            .system
            .config()
            .handler_name::<A, M>()
            .expect("message not setup for remoting");

        let (tx, rx) = oneshot::channel();

        let actor_id = self.actor_id.clone();
        self.sharding.host.notify(EntityRequest {
            actor_id,
            message_type,
            message,
            recipe: self.recipe.clone(),
            result_channel: Some(tx),
        });

        let result = rx.await;
        if let Ok(result) = result {
            let result = result
                .map(|res| M::read_remote_result(res).map_err(|res| ActorRefErr::ActorUnavailable));
            if let Ok(Ok(result)) = result {
                Ok(result)
            } else {
                // TODO: Better errors
                Err(ActorRefErr::ActorUnavailable)
            }
        } else {
            Err(ActorRefErr::ActorUnavailable)
        }
    }
}

pub fn sharding(builder: &mut RemoteActorHandlerBuilder) -> &mut RemoteActorHandlerBuilder {
    builder
        .with_handler::<ShardCoordinator, AllocateShard>("ShardCoordinator.AllocateShard")
        .with_handler::<ShardHost, ShardAllocated>("ShardHost.ShardAllocated")
        .with_handler::<Shard, RemoteEntityRequest>("Shard.RemoteEntityRequest")
}
