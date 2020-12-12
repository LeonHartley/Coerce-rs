pub mod util;

#[macro_use]
extern crate serde;

#[macro_use]
extern crate async_trait;

use coerce::actor::system::ActorSystem;
use coerce::actor::{Actor, ActorCreationErr, ActorState, Factory};
use coerce::remote::codec::json::JsonCodec;
use coerce::remote::codec::MessageCodec;
use coerce::remote::net::message::{ActorCreated, CreateActor};

use coerce::remote::storage::state::ActorStore;
use coerce::remote::system::RemoteActorSystem;
use uuid::Uuid;

pub struct TestActorStore {
    state: Option<ActorState>,
}

pub struct TestActor {
    name: String,
}

#[derive(Serialize, Deserialize)]
pub struct TestActorRecipe {
    name: String,
}

#[derive(Clone)]
pub struct TestActorFactory;

#[async_trait]
impl Factory for TestActorFactory {
    type Actor = TestActor;
    type Recipe = TestActorRecipe;

    async fn create(&self, recipe: Self::Recipe) -> Result<TestActor, ActorCreationErr> {
        log::info!("recipe create :D");
        // could do some mad shit like look in the db for the user data etc, if fails - fail the actor creation
        Ok(TestActor { name: recipe.name })
    }
}

impl Actor for TestActor {}

#[tokio::test]
pub async fn test_remote_actor_create_new() {
    util::create_trace_logger();

    let actor_id = format!("{}", Uuid::new_v4());
    let expected_actor_name = "test-actor-123".to_string();
    let recipe = format!("{{\"name\": \"{}\"}}", &expected_actor_name).into_bytes();

    let codec = JsonCodec::new();
    let system = ActorSystem::new();

    let factory = TestActorFactory {};
    let mut remote = RemoteActorSystem::builder()
        .with_actor_system(system)
        .with_actors(|builder| builder.with_actor::<TestActorFactory>("TestActor", factory))
        .build()
        .await;

    let message = CreateActor {
        id: Uuid::new_v4(),
        actor_id: Some(actor_id.clone()),
        actor_type: String::from("TestActor"),
        recipe,
    };

    let result = remote.handle_create_actor(message).await;
    let create_actor_res = codec.decode_msg::<ActorCreated>(result.unwrap()).unwrap();

    let mut actor = remote
        .inner()
        .get_tracked_actor::<TestActor>(actor_id.clone())
        .await
        .unwrap();
    let actor_name = actor.exec(|a| a.name.clone()).await.unwrap();

    assert_eq!(&create_actor_res.id, &actor_id);
    assert_eq!(create_actor_res.node_id, remote.node_id());
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
//     let actor_id = format!("{}", Uuid::new_v4());
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
