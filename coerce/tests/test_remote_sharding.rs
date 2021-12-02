use crate::util::{
    GetStatusRequest, GetStatusResponse, SetStatusRequest, TestActor, TestActorStatus,
};
use crate::TestActorStatus::Active;
use coerce::actor::message::{Envelope, Message};
use coerce::actor::system::ActorSystem;
use coerce::actor::{Actor, ActorCreationErr, ActorFactory, ActorRecipe, ActorRef, IntoActor};
use coerce::persistent::journal::provider::inmemory::InMemoryStorageProvider;
use coerce::persistent::{ConfigurePersistence, Persistence};
use coerce::remote::cluster::sharding::coordinator::allocation::AllocateShard;
use coerce::remote::cluster::sharding::coordinator::{ShardCoordinator, ShardHostState};
use coerce::remote::cluster::sharding::host::{ShardHost, StartEntity};
use coerce::remote::system::RemoteActorSystem;
use tokio::sync::oneshot;
use coerce::remote::cluster::sharding::host::request::EntityRequest;
use coerce::remote::cluster::sharding::host::stats::GetStats;

pub mod util;

#[macro_use]
extern crate serde;

#[macro_use]
extern crate async_trait;

#[macro_use]
extern crate coerce_macros;

pub struct TestActorRecipe;

impl ActorRecipe for TestActorRecipe {
    fn read_from_bytes(_bytes: Vec<u8>) -> Option<Self> {
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

    async fn create(&self, _recipe: Self::Recipe) -> Result<Self::Actor, ActorCreationErr> {
        Ok(TestActor {
            status: None,
            counter: 0,
        })
    }
}

#[tokio::test]
pub async fn test_shard_coordinator_shard_allocation() {
    const SHARD_ID: u32 = 1;

    util::create_trace_logger();
    let sys = ActorSystem::new().add_persistence(Persistence::from(InMemoryStorageProvider::new()));
    let remote = RemoteActorSystem::builder()
        .with_actor_system(sys)
        .with_tag("system-one")
        .with_actors(|a| a.with_actor(TestActorFactory))
        .with_id(1)
        .build()
        .await;

    let shard_host: ActorRef<ShardHost> = ShardHost::new(TestActor::type_name().to_string())
        .into_actor(Some("ShardHost".to_string()), &remote.actor_system())
        .await
        .expect("ShardHost start")
        .into();

    let mut shard_coordinator = ShardCoordinator::new(TestActor::type_name().to_string());
    shard_coordinator.add_host(ShardHostState {
        node_id: 1,
        node_tag: "system-one".to_string(),
        shards: Default::default(),
        actor: shard_host.clone(),
    });

    let shard_coordinator = shard_coordinator
        .into_actor(Some("ShardCoordinator".to_string()), &remote.actor_system())
        .await
        .expect("ShardCoordinator start");

    let allocation = shard_coordinator
        .send(AllocateShard { shard_id: SHARD_ID })
        .await;
    allocation.expect("shard allocation");

    let host_stats = shard_host.send(GetStats).await.expect("get host stats");
    assert_eq!(host_stats.hosted_shards, [SHARD_ID]);
}

#[tokio::test]
pub async fn test_shard_host_actor_request() {
    const SHARD_ID: u32 = 99;

    util::create_trace_logger();
    let sys = ActorSystem::new().add_persistence(Persistence::from(InMemoryStorageProvider::new()));
    let remote = RemoteActorSystem::builder()
        .with_actor_system(sys)
        .with_tag("system-one")
        .with_actors(|a| {
            a.with_actor(TestActorFactory)
                .with_handler::<TestActor, GetStatusRequest>("GetStatusRequest")
                .with_handler::<TestActor, SetStatusRequest>("SetStatusRequest")
        })
        .with_id(1)
        .build()
        .await;

    let shard_host: ActorRef<ShardHost> = ShardHost::new(TestActor::type_name().to_string())
        .into_actor(Some("ShardHost".to_string()), &remote.actor_system())
        .await
        .expect("ShardHost start")
        .into();

    let mut shard_coordinator = ShardCoordinator::new(TestActor::type_name().to_string());
    shard_coordinator.add_host(ShardHostState {
        node_id: 1,
        node_tag: "system-one".to_string(),
        shards: Default::default(),
        actor: shard_host.clone(),
    });

    let shard_coordinator = shard_coordinator
        .into_actor(Some("ShardCoordinator".to_string()), &remote.actor_system())
        .await
        .expect("ShardCoordinator start");

    let allocation = shard_coordinator
        .send(AllocateShard { shard_id: SHARD_ID })
        .await;

    allocation.expect("shard allocation");

    let (tx, rx) = oneshot::channel();

    let expected_status = TestActorStatus::Active;
    let set_status = SetStatusRequest {
        status: expected_status.clone(),
    }
    .as_remote_envelope()
        .unwrap();

    let _ = shard_host
        .notify(EntityRequest {
            actor_id: "leon".to_string(),
            message_type: "SetStatusRequest".to_string(),
            message: match set_status {
                Envelope::Remote(b) => b,
                _ => unreachable!(),
            },
            recipe: Some(vec![]),
            result_channel: None,
        })
        .await
        .expect("notify shard host");

    let get_status = GetStatusRequest().as_remote_envelope().unwrap();
    let _ = shard_host
        .notify(EntityRequest {
            actor_id: "leon".to_string(),
            message_type: "GetStatusRequest".to_string(),
            message: match get_status {
                Envelope::Remote(b) => b,
                _ => unreachable!(),
            },
            recipe: Some(vec![]),
            result_channel: Some(tx),
        })
        .await
        .expect("notify shard host");

    let result = rx.await.expect("channel recv").expect("msg result");
    let result = GetStatusRequest::read_remote_result(result).unwrap();
    assert_eq!(result, GetStatusResponse::Ok(expected_status));

    // let sharding = Sharding::<TestActor>::start(&system).await;
    // let sharded_actor = sharding.get("leon".to_string(), None);
    // sharded_actor
    //     .notify(SetStatusRequest {
    //         status: TestActorStatus::Active,
    //     })
    //     .await;
    //
    // let res = sharded_actor
    //     .send(GetStatusRequest())
    //     .await
    //     .expect("get status");
    // assert_eq!(res, GetStatusResponse::Ok(expected_status));
}
