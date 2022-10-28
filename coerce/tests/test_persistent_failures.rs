use coerce::actor::context::ActorContext;
use coerce::actor::message::Handler;
use coerce::actor::system::ActorSystem;
use coerce::actor::{IntoActor, ToActorId};
use coerce::persistent::journal::provider::inmemory::InMemoryStorageProvider;
use coerce::persistent::journal::provider::StorageProvider;
use coerce::persistent::journal::storage::{JournalEntry, JournalStorage, JournalStorageRef};
use coerce::persistent::journal::types::JournalTypes;
use coerce::persistent::{Persistence, PersistentActor, Recover};
use coerce_macros::JsonMessage;
use parking_lot::Mutex;
use protobuf::MessageField;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;
use tracing::info;

#[macro_use]
extern crate serde;

#[macro_use]
extern crate async_trait;

pub mod util;

#[derive(Default)]
struct PersistenceState {
    messages: Option<Vec<JournalEntry>>,
    snapshot: Option<JournalEntry>,
    next_errors: VecDeque<anyhow::Error>,
}

#[derive(Default)]
pub struct MockPersistence {
    state: Mutex<PersistenceState>,
}

struct Provider(Arc<MockPersistence>);

impl StorageProvider for Provider {
    fn journal_storage(&self) -> Option<JournalStorageRef> {
        Some(self.0.clone())
    }
}

struct TestActor {
    recovered_message: Option<Message>,
}

#[derive(JsonMessage, Serialize, Deserialize)]
#[result("()")]
struct Message;

impl PersistentActor for TestActor {
    fn configure(types: &mut JournalTypes<Self>) {
        types.message::<Message>("Message");
    }
}

#[async_trait]
impl Recover<Message> for TestActor {
    async fn recover(&mut self, message: Message, _ctx: &mut ActorContext) {
        self.recovered_message = Some(message);
    }
}

#[async_trait]
impl Handler<Message> for TestActor {
    async fn handle(&mut self, message: Message, ctx: &mut ActorContext) {
        info!("received message");
    }
}

#[derive(Debug)]
struct MockErr;

impl Display for MockErr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Mock error")
    }
}

impl Error for MockErr {}

#[tokio::test]
pub async fn test_persistent_actor_recovery_failure_handling() {
    util::create_trace_logger();

    let persistence = Arc::new(MockPersistence::default());
    let provider = Provider(persistence.clone());
    let system = ActorSystem::new().to_persistent(Persistence::from(provider));

    persistence.set_next_err(MockErr.into());

    let actor = TestActor {
        recovered_message: None,
    }
    .into_actor(Some("TestActor".to_actor_id()), &system)
    .await
    .unwrap();

    info!("actor created")
}

impl MockPersistence {
    pub fn set_next_err(&self, err: anyhow::Error) {
        let mut state = self.state.lock();
        state.next_errors.push(err);
    }
}

#[async_trait]
impl JournalStorage for MockPersistence {
    async fn write_snapshot(
        &self,
        persistence_id: &str,
        entry: JournalEntry,
    ) -> anyhow::Result<()> {
        let mut state = self.state.lock();
        if let Some(error) = state.next_errors.take() {
            Err(error)
        } else {
            Ok(())
        }
    }

    async fn write_message(&self, persistence_id: &str, entry: JournalEntry) -> anyhow::Result<()> {
        let mut state = self.state.lock();
        if let Some(error) = state.next_error.take() {
            Err(error)
        } else {
            Ok(())
        }
    }

    async fn read_latest_snapshot(
        &self,
        persistence_id: &str,
    ) -> anyhow::Result<Option<JournalEntry>> {
        let mut state = self.state.lock();
        if let Some(error) = state.next_error.take() {
            Err(error)
        } else {
            Ok(state.snapshot.clone())
        }
    }

    async fn read_latest_messages(
        &self,
        persistence_id: &str,
        from_sequence: i64,
    ) -> anyhow::Result<Option<Vec<JournalEntry>>> {
        let mut state = self.state.lock();
        if let Some(error) = state.next_error.take() {
            Err(error)
        } else {
            Ok(state.messages.clone())
        }
    }

    async fn delete_all(&self, persistence_id: &str) -> anyhow::Result<()> {
        Ok(())
    }
}
