pub mod util;

#[macro_use]
extern crate serde;
extern crate serde_json;

extern crate chrono;

#[macro_use]
extern crate async_trait;

use coerce_remote::storage::activator::ActorActivator;
use coerce_remote::storage::state::StatefulActor;
use coerce_remote::storage::state::{ActorState, ActorStore, ActorStoreErr};
use coerce_rt::actor::Actor;
use std::convert::{TryFrom, TryInto};
use uuid::Uuid;

pub struct TestActorStore {
    state: Option<ActorState>,
}

#[async_trait]
impl ActorStore for TestActorStore {
    async fn get(&mut self, actor_id: Uuid) -> Result<Option<ActorState>, ActorStoreErr> {
        Ok(self.state.clone())
    }

    async fn put(&mut self, actor: &ActorState) -> Result<(), ActorStoreErr> {
        unimplemented!()
    }

    async fn remove(&mut self, actor_id: Uuid) -> Result<bool, ActorStoreErr> {
        unimplemented!()
    }

    fn clone(&self) -> Box<dyn ActorStore + Sync + Send> {
        unimplemented!()
    }
}

pub struct TestActor {}

impl Actor for TestActor {}

impl TryFrom<ActorState> for TestActor {
    type Error = ();

    fn try_from(value: ActorState) -> Result<Self, Self::Error> {
        Ok(TestActor {})
    }
}

impl TryInto<ActorState> for TestActor {
    type Error = ();

    fn try_into(self) -> Result<ActorState, Self::Error> {
        Ok(ActorState {
            actor_id: Default::default(),
            state: vec![],
        })
    }
}

#[tokio::test]
pub async fn test_remote_actor_activator() {
    util::create_trace_logger();

    let actor_id = Uuid::new_v4();
    let store = TestActorStore {
        state: Some(ActorState {
            actor_id,
            state: vec![1, 2, 3],
        }),
    };

    let mut activator = ActorActivator::new(Box::new(store));
    let actor = activator.activate::<TestActor>(actor_id).await;
    if let Some(actor) = actor {

    } else {
        panic!("no actor created")
    }
}
