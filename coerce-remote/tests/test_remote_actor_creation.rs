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

use uuid::Uuid;

pub struct TestActorStore {
    state: Option<ActorState>,
}

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

#[derive(Deserialize)]
pub struct TestActor {
    name: String,
}

impl Actor for TestActor {}

#[tokio::test]
pub async fn test_remote_actor_activator() {
    util::create_trace_logger();

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
