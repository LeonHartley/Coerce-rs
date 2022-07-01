use crate::util::{GetStatusRequest, SetStatusRequest, TestActor, TestActorStatus};

use coerce::actor::system::ActorSystem;
use coerce::actor::{ActorCreationErr, ActorFactory, ActorRecipe};
use coerce::persistent::journal::provider::inmemory::InMemoryStorageProvider;
use coerce::persistent::{ConfigurePersistence, Persistence};
use coerce::remote::api::cluster::ClusterApi;
use coerce::remote::api::sharding::ShardingApi;
use coerce::remote::api::RemoteHttpApi;
use coerce::remote::cluster::sharding::Sharding;
use coerce::remote::net::server::RemoteServer;
use coerce::remote::system::RemoteActorSystem;
use std::net::SocketAddr;
use std::str::FromStr;

mod util;

#[macro_use]
extern crate serde;

#[macro_use]
extern crate async_trait;

#[macro_use]
extern crate coerce_macros;

#[derive(Clone)]
struct TestActorFactory {}

struct TestActorRecipe;

impl ActorRecipe for TestActorRecipe {
    fn read_from_bytes(_bytes: &Vec<u8>) -> Option<Self> {
        Some(Self)
    }

    fn write_to_bytes(&self) -> Option<Vec<u8>> {
        Some(vec![])
    }
}
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
pub async fn test_remote_api_routes() {
    util::create_trace_logger();

    let persistence = Persistence::from(InMemoryStorageProvider::new());
    async fn create_system(persistence: Persistence) -> (RemoteActorSystem, RemoteServer) {
        let sys = ActorSystem::new().add_persistence(persistence);
        let remote = RemoteActorSystem::builder()
            .with_actor_system(sys)
            .with_tag("system-one")
            .with_actors(|a| {
                a.with_actor(TestActorFactory {})
                    .with_handler::<TestActor, GetStatusRequest>("GetStatusRequest")
                    .with_handler::<TestActor, SetStatusRequest>("SetStatusRequest")
            })
            .with_id(1)
            .single_node()
            .build()
            .await;

        let server = remote
            .clone()
            .cluster_worker()
            .listen_addr("localhost:30101")
            .start()
            .await;

        (remote, server)
    }

    let (remote, _server) = create_system(persistence).await;
    let sharding = Sharding::<TestActorFactory>::builder(remote.clone())
        .with_entity_type("TestActor")
        .build()
        .await;

    let cluster_api = ClusterApi::new(remote.clone());
    let sharding_api = ShardingApi::default()
        .attach(&sharding)
        .start(remote.actor_system())
        .await;

    let _ = tokio::spawn(
        RemoteHttpApi::new(
            SocketAddr::from_str("0.0.0.0:3000").unwrap(),
            remote.clone(),
        )
        .routes(&cluster_api)
        .routes(&sharding_api)
        .start(),
    );

    let sharded_actor = sharding.get("leon".to_string(), Some(TestActorRecipe));
    let _res = sharded_actor
        .send(SetStatusRequest {
            status: TestActorStatus::Active,
        })
        .await;

    // ctrl_c().await;
}
