pub mod util;

#[macro_use]
extern crate serde;

#[macro_use]
extern crate async_trait;

use coerce::actor::system::ActorSystem;
use coerce::actor::{new_actor_id, Actor, ActorCreationErr, ActorFactory, ActorRecipe};
use coerce::remote::net::proto::protocol::{ActorAddress, CreateActor};
use coerce::remote::system::{RemoteActorErr, RemoteActorSystem};
use protobuf::Message;
use std::time::Duration;
use uuid::Uuid;

pub struct TestActor {
    name: String,
}

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
impl ActorFactory for TestActorFactory {
    type Actor = TestActor;
    type Recipe = TestActorRecipe;

    async fn create(&self, recipe: Self::Recipe) -> Result<TestActor, ActorCreationErr> {
        log::trace!("recipe create :D");
        // could do some mad shit like look in the db for the user data etc, if fails - fail the actor creation
        Ok(TestActor { name: recipe.name })
    }
}

impl Actor for TestActor {}

#[tokio::test]
pub async fn test_remote_actor_deploy_remotely() {
    util::create_trace_logger();

    let expected_actor_name = "test-actor-123".to_string();
    let actor_id = expected_actor_name.clone();

    let sys = ActorSystem::new();
    let remote = RemoteActorSystem::builder()
        .with_actor_system(sys)
        .with_actors(|builder| builder.with_actor::<TestActorFactory>(TestActorFactory {}))
        .with_tag("system-a")
        .with_id(1)
        .build()
        .await;

    let sys = ActorSystem::new();
    let remote_b = RemoteActorSystem::builder()
        .with_actor_system(sys)
        .with_tag("system-b")
        .with_id(2)
        .build()
        .await;

    remote
        .clone()
        .cluster_worker()
        .listen_addr("localhost:30101")
        .start()
        .await;

    remote_b
        .clone()
        .cluster_worker()
        .listen_addr("localhost:30102")
        .with_seed_addr("localhost:30101")
        .start()
        .await;

    let recipe = TestActorRecipe {
        name: expected_actor_name,
    };

    let deployment_result = remote_b
        .deploy_actor::<TestActorFactory>(Some(actor_id.clone()), recipe, Some(remote.node_id()))
        .await;

    assert_eq!(deployment_result.is_ok(), true)
}

#[tokio::test]
pub async fn test_remote_actor_create_new_locally() {
    let actor_id = new_actor_id();
    let expected_actor_name = "test-actor-123".to_string();

    let system = ActorSystem::new();

    let factory = TestActorFactory {};
    let remote = RemoteActorSystem::builder()
        .with_actor_system(system)
        .with_actors(|builder| builder.with_actor::<TestActorFactory>(factory))
        .build()
        .await;

    let result = remote
        .deploy_actor::<TestActorFactory>(
            Some(actor_id.clone()),
            TestActorRecipe {
                name: expected_actor_name.clone(),
            },
            None,
        )
        .await;
    let duplicate = remote
        .deploy_actor::<TestActorFactory>(
            Some(actor_id.clone()),
            TestActorRecipe {
                name: expected_actor_name.clone(),
            },
            None,
        )
        .await;

    let create_actor_res = result.unwrap();

    let actor = remote
        .actor_system()
        .get_tracked_actor::<TestActor>(actor_id.clone())
        .await
        .unwrap();

    let actor_name = actor.exec(|a| a.name.clone()).await.unwrap();
    let node = remote.locate_actor_node(actor_id.clone()).await;

    assert_eq!(Some(RemoteActorErr::ActorExists), duplicate.err());
    assert_eq!(create_actor_res.actor_id(), &actor_id);
    assert_eq!(node.unwrap(), remote.node_id());
    assert_eq!(actor_name, expected_actor_name);
}
//
// #[tokio::test]
// pub async fn test_remote_actor_create_from_store() {
//     #[async_trait]
//     impl ActorStore for TestActorStore {
//         async fn get(&mut self, _actor_id: ActorId) -> Result<Option<ActorState>, ActorStoreErr> {
//             Ok(self.state.clone())
//         }
//
//         async fn put(&mut self, _actor: &ActorState) -> Result<(), ActorStoreErr> {
//             unimplemented!()
//         }
//
//         async fn remove(&mut self, _actor_id: ActorId) -> Result<bool, ActorStoreErr> {
//             unimplemented!()
//         }
//
//         fn clone(&self) -> Box<dyn ActorStore + Sync + Send> {
//             unimplemented!()
//         }
//     }
//
//     let expected_actor_name = "test-actor".to_string();
//     let actor_id = new_actor_id();
//     let store = TestActorStore {
//         state: Some(ActorState {
//             actor_id: actor_id.clone(),
//             state: format!("{{\"name\": \"{}\"}}", &expected_actor_name).into_bytes(),
//         }),
//     };
//
//     let mut activator = ActorActivator::new(Box::new(store));
//     let actor = activator.activate::<TestActor>(&actor_id).await;
//     if let Some(actor) = actor {
//         assert_eq!(actor.name, expected_actor_name)
//     } else {
//         panic!("no actor created")
//     }
// }
