use coerce::actor::context::ActorContext;
use coerce::actor::lifecycle::Stop;
use coerce::actor::message::{Envelope, Handler, Message, MessageUnwrapErr, MessageWrapErr};
use coerce::actor::system::ActorSystem;
use coerce::actor::{IntoActor, LocalActorRef};
use coerce::persistent::context::ActorPersistence;
use coerce::persistent::journal::provider::inmemory::InMemoryStorageProvider;
use coerce::persistent::journal::provider::StorageProvider;
use coerce::persistent::journal::snapshot::Snapshot;
use coerce::persistent::journal::storage::{JournalEntry, JournalStorage, JournalStorageRef};
use coerce::persistent::journal::types::JournalTypes;
use coerce::persistent::{
    ConfigurePersistence, Persistence, PersistentActor, Recover, RecoverSnapshot,
};
use coerce_macros::{JsonMessage, JsonSnapshot};
use std::any::Any;
use std::panic::PanicInfo;
use std::sync::Arc;
use uuid::Uuid;

#[macro_use]
extern crate serde;

#[macro_use]
extern crate async_trait;

#[macro_use]
extern crate slog;



#[async_trait]
impl RecoverSnapshot<TestActorSnapshot> for TestActor {
    async fn recover(&mut self, _snapshot: TestActorSnapshot, ctx: &mut ActorContext) {
        info!(ctx.log(), "recovered a snapshot");
    }
}

pub mod util;

struct TestActor {
    id: i64,
    received_numbers: Vec<i32>,
}

#[derive(JsonMessage, Serialize, Deserialize)]
#[result("()")]
struct Msg(i32);

#[derive(JsonSnapshot, Serialize, Deserialize)]
struct TestActorSnapshot {}

#[async_trait]
impl PersistentActor for TestActor {
    fn persistence_key(&self, ctx: &ActorContext) -> String {
        format!("test-actor-{}", &self.id)
    }

    fn configure(journal: &mut JournalTypes<Self>) {
        journal
            .snapshot::<TestActorSnapshot>("test-snapshot")
            .message::<Msg>("test-message");
    }
}

#[async_trait]
impl Handler<Msg> for TestActor {
    async fn handle(&mut self, message: Msg, ctx: &mut ActorContext) {
        if self.persist(&message, ctx).await.is_ok() {
            info!(ctx.log(), "persist ok, number: {}", message.0);
            self.received_numbers.push(message.0);
        } else {
            // NACK
        }
    }
}

#[async_trait]
impl Recover<Msg> for TestActor {
    async fn recover(&mut self, message: Msg, ctx: &mut ActorContext) {
<<<<<<< Updated upstream
        info!("recovered a number: {}", message.0);
=======
        info!(ctx.log(), "recovered a number: {}", message.0);
>>>>>>> Stashed changes
        self.received_numbers.push(message.0);
    }
}

#[tokio::test]
pub async fn test_persistent_actor_message_recovery() {
    util::create_trace_logger();

<<<<<<< Updated upstream
    let mut system =
        ActorSystem::new().add_persistence(Persistence::from(InMemoryStorageProvider::new()));
=======
    let system = ActorSystem::new();
    let log = system.log().clone();
    let system = system.add_persistence(Persistence::from(InMemoryStorageProvider::new(log)));
>>>>>>> Stashed changes

    let id = 1;
    let create_empty_actor = || TestActor {
        id,
        received_numbers: vec![],
    };
    let actor = create_empty_actor()
        .into_actor(Some("hi".to_string()), &system)
        .await
        .expect("create actor");

    actor.notify(Msg(1)).unwrap();
    actor.notify(Msg(2)).unwrap();
    actor.notify(Msg(3)).unwrap();
    actor.notify(Msg(4)).unwrap();

    assert!(actor
        .exec(|a| {
            a.received_numbers == vec![1, 2, 3, 4]
        })
        .await
        .unwrap());

    actor.stop().await.unwrap();

    let actor = create_empty_actor()
        .into_actor(Some("hi".to_string()), &system)
        .await;

    assert!(actor
        .unwrap()
        .exec(|a| {
            a.received_numbers == vec![1, 2, 3, 4]
        })
        .await
        .unwrap());

    system.shutdown().await;
}
