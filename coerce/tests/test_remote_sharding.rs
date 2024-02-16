use crate::util::{
    GetStatusRequest, GetStatusResponse, SetStatusRequest, TestActor, TestActorStatus,
};

use coerce::actor::message::Message;
use coerce::actor::system::ActorSystem;
use coerce::actor::{
    Actor, ActorCreationErr, ActorFactory, ActorRecipe, ActorRef, IntoActor, LocalActorRef,
};
use coerce::persistent::journal::provider::inmemory::InMemoryStorageProvider;
use coerce::persistent::Persistence;
use coerce::sharding::coordinator::allocation::{AllocateShard, AllocateShardResult};
use coerce::sharding::coordinator::{ShardCoordinator, ShardHostState, ShardHostStatus, ShardId};

use coerce::remote::handler::{ActorHandler, RemoteActorHandler};
use coerce::remote::net::server::RemoteServer;
use coerce::remote::system::{NodeId, RemoteActorSystem};
use coerce::sharding::host::stats::GetStats;
use coerce::sharding::host::ShardHost;
use coerce::sharding::Sharding;

mod sharding;
pub mod util;

#[macro_use]
extern crate serde;

#[macro_use]
extern crate async_trait;

#[macro_use]
extern crate coerce_macros;

pub struct TestActorRecipe;

impl ActorRecipe for TestActorRecipe {
    fn read_from_bytes(_bytes: &Vec<u8>) -> Option<Self> {
        Some(Self)
    }

    fn write_to_bytes(&self) -> Option<Vec<u8>> {
        Some(vec![])
    }
}

#[derive(Clone)]
pub struct TestActorFactory;

#[async_trait]
impl ActorFactory for TestActorFactory {
    type Actor = TestActor;
    type Recipe = TestActorRecipe;

    async fn create(&self, _recipe: TestActorRecipe) -> Result<TestActor, ActorCreationErr> {
        Ok(TestActor {
            status: None,
            counter: 0,
        })
    }
}

async fn create_shard_coordinator<T: Actor>(
    remote: &RemoteActorSystem,
    node_id: NodeId,
    node_tag: String,
    shard_host: ActorRef<ShardHost>,
) -> LocalActorRef<ShardCoordinator> {
    let mut shard_coordinator = ShardCoordinator::new(
        T::type_name().to_string(),
        shard_host.clone().unwrap_local(),
    );

    shard_coordinator.add_host(ShardHostState {
        node_id,
        node_tag,
        shards: Default::default(),
        actor: shard_host,
        status: ShardHostStatus::Ready,
    });

    let shard_coordinator = shard_coordinator
        .into_actor(Some("shard-coordinator".to_string()), remote.actor_system())
        .await
        .expect("ShardCoordinator start");

    shard_coordinator
}

#[tokio::test]
pub async fn test_shard_coordinator_shard_allocation() {
    const SHARD_ID: u32 = 1;

    util::create_trace_logger();

    let handler = RemoteActorHandler::<TestActor, TestActorFactory>::new(TestActorFactory {});

    let sys = ActorSystem::new().to_persistent(Persistence::from(InMemoryStorageProvider::new()));
    let remote = RemoteActorSystem::builder()
        .with_actor_system(sys)
        .with_tag("system-one")
        .with_actors(|a| a.with_actor(TestActorFactory))
        .with_id(1)
        .build()
        .await;

    let shard_host: ActorRef<ShardHost> = ShardHost::new(
        TestActor::type_name().to_string(),
        handler.new_boxed(),
        None,
    )
    .into_actor(Some("ShardHost".to_string()), remote.actor_system())
    .await
    .expect("ShardHost start")
    .into();

    let shard_coordinator = create_shard_coordinator::<TestActor>(
        &remote,
        1,
        "system-one".to_string(),
        shard_host.clone(),
    )
    .await;

    let allocation = shard_coordinator
        .send(AllocateShard {
            shard_id: SHARD_ID,
            rebalancing: false,
        })
        .await;

    let initial_allocation = allocation.expect("shard allocation");
    let res = shard_coordinator.stop().await;
    assert!(res.is_ok());

    let host_stats = shard_host
        .send(GetStats)
        .await
        .unwrap()
        .await
        .expect("get host stats");
    let _ = shard_host.unwrap_local().stop().await;

    let shard_host: ActorRef<ShardHost> = ShardHost::new(
        TestActor::type_name().to_string(),
        handler.new_boxed(),
        None,
    )
    .into_actor(Some("shard-host".to_string()), remote.actor_system())
    .await
    .expect("ShardHost start")
    .into();

    let shard_coordinator = create_shard_coordinator::<TestActor>(
        &remote,
        1,
        "system-one".to_string(),
        shard_host.clone(),
    )
    .await;

    let allocation = shard_coordinator
        .send(AllocateShard {
            shard_id: SHARD_ID,
            rebalancing: false,
        })
        .await;

    let allocation_after_restart = allocation.expect("shard allocation");
    assert_eq!(
        host_stats
            .hosted_shards
            .keys()
            .copied()
            .collect::<Vec<ShardId>>(),
        [SHARD_ID]
    );

    assert_eq!(
        initial_allocation,
        AllocateShardResult::Allocated(SHARD_ID, 1)
    );
    assert_eq!(
        allocation_after_restart,
        AllocateShardResult::AlreadyAllocated(SHARD_ID, 1)
    );
}

#[tokio::test]
pub async fn test_shard_host_actor_request() {
    const SHARD_ID: u32 = 99;

    util::create_trace_logger();

    let persistence = Persistence::from(InMemoryStorageProvider::new());

    async fn create_system(persistence: Persistence) -> (RemoteActorSystem, RemoteServer) {
        let sys = ActorSystem::new().to_persistent(persistence);
        let remote = RemoteActorSystem::builder()
            .with_actor_system(sys)
            .with_tag("system-one")
            .with_actors(|a| {
                a.with_actor(TestActorFactory)
                    .with_handler::<TestActor, GetStatusRequest>("GetStatusRequest")
                    .with_handler::<TestActor, SetStatusRequest>("SetStatusRequest")
            })
            .with_id(1)
            // .single_node()
            .build()
            .await;

        let server = remote
            .clone()
            .cluster_worker()
            .listen_addr("0.0.0.0:30101")
            .start()
            .await;

        (remote, server)
    }

    let (remote, server) = create_system(persistence.clone()).await;

    let sharding = Sharding::<TestActorFactory>::builder(remote.clone())
        .build()
        .await;
    let sharded_actor = sharding.get("leon".to_string(), Some(TestActorRecipe));

    let _ = sharded_actor
        .send(SetStatusRequest {
            status: TestActorStatus::Active,
        })
        .await;

    let res = sharded_actor
        .send(GetStatusRequest)
        .await
        .expect("get status");

    let expected_status = TestActorStatus::Active;
    assert_eq!(res, GetStatusResponse::Ok(expected_status));

    // stop the system, and start a new one (sharing the same persistence backplane)

    let server = server;
    server.stop();
    remote.actor_system().shutdown().await;

    let (remote, _server) = create_system(persistence.clone()).await;

    let sharding = Sharding::<TestActorFactory>::builder(remote.clone())
        .build()
        .await;

    // create a reference to the sharded actor without specifying a recipe, which stops the sharding internals from creating the actor if it isn't already running
    let sharded_actor = sharding.get("leon".to_string(), None);
    let res_after_system_restart = sharded_actor
        .send(SetStatusRequest {
            status: TestActorStatus::Active,
        })
        .await;

    assert_eq!(res_after_system_restart.is_ok(), true);
}
