pub mod util;

#[macro_use]
extern crate serde;

#[macro_use]
extern crate async_trait;

#[macro_use]
extern crate coerce_macros;

use coerce::actor::system::ActorSystem;
use coerce::actor::{ActorCreationErr, ActorFactory, ActorRecipe};

use coerce::remote::system::RemoteActorSystem;
use util::*;

#[derive(Serialize, Deserialize)]
struct TestActorRecipe;

#[derive(Clone)]
struct TestActorFactory;

impl ActorRecipe for TestActorRecipe {
    fn read_from_bytes(bytes: &Vec<u8>) -> Option<Self> {
        serde_json::from_slice(&bytes).unwrap()
    }

    fn write_to_bytes(&self) -> Option<Vec<u8>> {
        serde_json::to_vec(&self).map_or(None, |b| Some(b))
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

#[coerce_test]
pub async fn test_remote_cluster_client_get_actor() {
    util::create_trace_logger();

    let system = ActorSystem::new();
    let remote = RemoteActorSystem::builder()
        .with_actor_system(system)
        .with_handlers(|builder| builder.with_actor::<TestActorFactory>(TestActorFactory {}))
        .build()
        .await;

    let created_actor = remote
        .actor_system()
        .new_tracked_actor(TestActor::new())
        .await
        .unwrap();

    let actor_id = created_actor.id;
    let actor = remote.actor_ref::<TestActor>(actor_id).await;
    assert_eq!(actor.is_some(), true);
}
//
// #[coerce_test]
// pub async fn test_remote_cluster_client_create_actor() {
//     let mut system = ActorSystem::new();
//     let _actor = system.new_tracked_actor(TestActor::new()).await.unwrap();
//     let remote = RemoteActorSystem::builder()
//         .with_actor_system(system)
//         .with_handlers(|builder| {
//             builder.with_actor::<TestActorFactory>("TestActor", TestActorFactory {})
//         })
//         .build()
//         .await;
//
//     let mut client = remote.cluster_client().build();
//
//     let actor = client
//         .create_actor::<TestActorFactory>(TestActorRecipe {}, Some(format!("TestActor")))
//         .await;
//
//     assert_eq!(actor.is_some(), false);
// }
