pub mod util;

#[macro_use]
extern crate serde;

#[macro_use]
extern crate async_trait;

#[macro_use]
extern crate coerce_macros;

use coerce::actor::system::ActorSystem;
use coerce::remote::system::builder::RemoteActorHandlerBuilder;
use coerce::remote::system::RemoteActorSystem;

use coerce::actor::{ActorCreationErr, ActorRecipe, Factory};
use util::*;
use tokio::time::Duration;

#[derive(Serialize, Deserialize)]
pub struct TestActorRecipe {
    name: String,
}

impl ActorRecipe for TestActorRecipe {
    fn read_from_bytes(bytes: Vec<u8>) -> Option<Self> {
        serde_json::from_slice(&bytes).unwrap()
    }

    fn write_to_bytes(&self) -> Option<Vec<u8>> {
        serde_json::to_vec(&self).map_or(None, |b| Some(b))
    }
}

#[derive(Clone)]
pub struct TestActorFactory;

#[async_trait]
impl Factory for TestActorFactory {
    type Actor = TestActor;
    type Recipe = TestActorRecipe;

    async fn create(&self, _recipe: Self::Recipe) -> Result<TestActor, ActorCreationErr> {
        log::trace!("recipe create :D");
        // could do some mad shit like look in the db for the user data etc, if fails - fail the actor creation
        Ok(TestActor {
            status: None,
            counter: 0,
        })
    }
}

#[derive(Serialize, Deserialize)]
pub struct EchoActorRecipe {}

impl ActorRecipe for EchoActorRecipe {
    fn read_from_bytes(bytes: Vec<u8>) -> Option<Self> {
        serde_json::from_slice(&bytes).unwrap()
    }

    fn write_to_bytes(&self) -> Option<Vec<u8>> {
        serde_json::to_vec(&self).map_or(None, |b| Some(b))
    }
}

#[derive(Clone)]
pub struct EchoActorFactory;

#[async_trait]
impl Factory for EchoActorFactory {
    type Actor = EchoActor;
    type Recipe = EchoActorRecipe;

    async fn create(&self, _recipe: Self::Recipe) -> Result<EchoActor, ActorCreationErr> {
        log::trace!("recipe create :D");
        // could do some mad shit like look in the db for the user data etc, if fails - fail the actor creation
        Ok(EchoActor {})
    }
}

#[coerce_test]
pub async fn test_remote_cluster_worker_builder() {
    util::create_trace_logger();

    let mut system = ActorSystem::new();
    let _actor = system.new_tracked_actor(TestActor::new()).await.unwrap();
    let remote = RemoteActorSystem::builder()
        .with_actor_system(system)
        .with_handlers(build_handlers)
        .build()
        .await;

    let mut remote_c = remote.clone();

    let remote_2 = RemoteActorSystem::builder()
        .with_actor_system(ActorSystem::new())
        .with_handlers(build_handlers)
        .build()
        .await;

    let mut remote_2_c = remote_2.clone();

    let remote_3 = RemoteActorSystem::builder()
        .with_actor_system(ActorSystem::new())
        .with_handlers(build_handlers)
        .build()
        .await;

    let mut remote_3_c = remote_3.clone();

    remote
        .cluster_worker()
        .listen_addr("localhost:30101")
        .start()
        .await;

    remote_2
        .cluster_worker()
        .listen_addr("localhost:30102")
        .with_seed_addr("localhost:30101")
        .start()
        .await;

    remote_3
        .cluster_worker()
        .listen_addr("localhost:30103")
        .with_seed_addr("localhost:30101")
        .start()
        .await;

    // TODO: remote actor registration is sometimes not instant, especially on resource limited environments like CI containers
    tokio::time::sleep(Duration::from_millis(10)).await;
    let nodes_a = remote_c.get_nodes().await;
    let nodes_b = remote_2_c.get_nodes().await;
    let nodes_c = remote_3_c.get_nodes().await;

    log::trace!("a: {:?}", &nodes_a);
    log::trace!("b: {:?}", &nodes_b);
    log::trace!("c: {:?}", &nodes_c);

    let nodes_a_in_b = nodes_a.iter().filter(|n| nodes_b.contains(n)).count();
    let nodes_a_in_c = nodes_a.iter().filter(|n| nodes_c.contains(n)).count();
    let nodes_b_in_a = nodes_b.iter().filter(|n| nodes_a.contains(n)).count();
    let nodes_b_in_c = nodes_b.iter().filter(|n| nodes_c.contains(n)).count();
    let nodes_c_in_a = nodes_c.iter().filter(|n| nodes_a.contains(n)).count();
    let nodes_c_in_b = nodes_c.iter().filter(|n| nodes_b.contains(n)).count();

    assert_eq!(nodes_a_in_b, nodes_a.len());
    assert_eq!(nodes_a_in_c, nodes_a.len());
    assert_eq!(nodes_b_in_a, nodes_b.len());
    assert_eq!(nodes_b_in_c, nodes_b.len());
    assert_eq!(nodes_c_in_a, nodes_c.len());
    assert_eq!(nodes_c_in_b, nodes_c.len());
}

fn build_handlers(handlers: &mut RemoteActorHandlerBuilder) -> &mut RemoteActorHandlerBuilder {
    handlers
        .with_actor::<TestActorFactory>("TestActor", TestActorFactory {})
        .with_actor::<EchoActorFactory>("EchoActor", EchoActorFactory {})
        .with_handler::<TestActor, SetStatusRequest>("TestActor.SetStatusRequest")
        .with_handler::<TestActor, GetStatusRequest>("TestActor.GetStatusRequest")
        .with_handler::<EchoActor, GetCounterRequest>("EchoActor.GetCounterRequest")
}
