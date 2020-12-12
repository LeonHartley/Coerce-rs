pub mod util;

#[macro_use]
extern crate serde;
extern crate serde_json;

extern crate chrono;

#[macro_use]
extern crate async_trait;

#[macro_use]
extern crate coerce_macros;

use coerce_rt::actor::context::ActorSystem;
use coerce_rt::actor::{ActorCreationErr, Factory};
use coerce_rt::remote::cluster::client::RemoteClusterClient;
use coerce_rt::remote::system::RemoteActorSystem;
use util::*;

#[derive(Serialize, Deserialize)]
struct TestActorRecipe;

#[derive(Clone)]
struct TestActorFactory;

#[async_trait]
impl Factory for TestActorFactory {
    type Actor = TestActor;
    type Recipe = TestActorRecipe;

    async fn create(&self, recipe: Self::Recipe) -> Result<Self::Actor, ActorCreationErr> {
        Ok(TestActor {
            status: None,
            counter: 0,
        })
    }
}

#[coerce_test]
pub async fn test_remote_cluster_client_builder() {
    util::create_trace_logger();

    let mut system = ActorSystem::new();
    let _actor = system.new_tracked_actor(TestActor::new()).await.unwrap();
    let remote = RemoteActorSystem::builder()
        .with_actor_system(system)
        .with_handlers(|builder| {
            builder.with_actor::<TestActorFactory>("TestActor", TestActorFactory {})
        })
        .build()
        .await;

    let mut client = remote.cluster_client().build();
    let actor = client.get_actor::<TestActor>(format!("leon")).await;

    assert_eq!(actor.is_some(), false);
}
