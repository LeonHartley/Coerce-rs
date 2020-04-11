pub mod util;

#[macro_use]
extern crate serde;
extern crate serde_json;

extern crate chrono;

#[macro_use]
extern crate async_trait;

use coerce_remote::storage::activator::ActorActivator;
use coerce_remote::storage::state::{ActorStore, ActorStoreErr};
use coerce_rt::actor::{Actor, ActorId, ActorState};

use coerce_remote::context::RemoteActorContext;
use coerce_rt::actor::context::ActorContext;
use uuid::Uuid;
use coerce_remote::net::message::{CreateActor, ActorCreated};
use coerce_remote::codec::json::JsonCodec;
use coerce_remote::codec::MessageCodec;

pub struct TestActorStore {
    state: Option<ActorState>,
}

#[derive(Deserialize)]
pub struct TestActor {
    name: String,
}

impl Actor for TestActor {}

#[tokio::test]
pub async fn test_remote_actor_create_new() {
    util::create_trace_logger();

    let actor_id = format!("{}", Uuid::new_v4());
    let expected_actor_name = "test-actor-123".to_string();
    let actor_state = format!("{{\"name\": \"{}\"}}", &expected_actor_name).into_bytes();

    let codec = JsonCodec::new();
    let mut context = ActorContext::new();
    let mut remote = RemoteActorContext::builder()
        .with_actor_context(context)
        .with_actors(|builder| builder.with_actor::<TestActor>("TestActor"))
        .build()
        .await;

    let message = CreateActor {
        id: Uuid::new_v4(),
        actor_id: Some(actor_id.clone()),
        actor_type: String::from("TestActor"),
        actor: actor_state
    };

    let result = remote.handle_create_actor(message).await;
    let create_actor_res = codec.decode_msg::<ActorCreated>(result.unwrap()).unwrap();

    let mut actor = remote.inner().get_tracked_actor::<TestActor>(actor_id.clone()).await.unwrap();
    let actor_name = actor.exec(|a| a.name.clone()).await.unwrap();

    assert_eq!(&create_actor_res.id, &actor_id);
    assert_eq!(create_actor_res.node_id, remote.node_id());
    assert_eq!(actor_name, expected_actor_name);
}

#[tokio::test]
pub async fn test_remote_actor_create_from_store() {
    #[async_trait]
    impl ActorStore for TestActorStore {
        async fn get(&mut self, _actor_id: ActorId) -> Result<Option<ActorState>, ActorStoreErr> {
            Ok(self.state.clone())
        }

        async fn put(&mut self, _actor: &ActorState) -> Result<(), ActorStoreErr> {
            unimplemented!()
        }

        async fn remove(&mut self, _actor_id: ActorId) -> Result<bool, ActorStoreErr> {
            unimplemented!()
        }

        fn clone(&self) -> Box<dyn ActorStore + Sync + Send> {
            unimplemented!()
        }
    }

    let expected_actor_name = "test-actor".to_string();
    let actor_id = format!("{}", Uuid::new_v4());
    let store = TestActorStore {
        state: Some(ActorState {
            actor_id: actor_id.clone(),
            state: format!("{{\"name\": \"{}\"}}", &expected_actor_name).into_bytes(),
        }),
    };

    let mut activator = ActorActivator::new(Box::new(store));
    let actor = activator.activate::<TestActor>(&actor_id).await;
    if let Some(actor) = actor {
        assert_eq!(actor.name, expected_actor_name)
    } else {
        panic!("no actor created")
    }
}
