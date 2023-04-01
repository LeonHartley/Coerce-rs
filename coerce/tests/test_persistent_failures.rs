use coerce::actor::context::ActorContext;
use coerce::actor::message::Handler;
use coerce::actor::system::ActorSystem;

use coerce::actor::{ActorRefErr, IntoActor, ToActorId};

use coerce::persistent::journal::provider::StorageProvider;
use coerce::persistent::journal::storage::{JournalEntry, JournalStorage, JournalStorageRef};
use coerce::persistent::journal::types::JournalTypes;
use coerce::persistent::{
    PersistFailurePolicy, Persistence, PersistentActor, Recover, RecoveryFailurePolicy, Retry,
};
use coerce_macros::JsonMessage;
use parking_lot::Mutex;

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
    recovery_policy: RecoveryFailurePolicy,
    persist_policy: PersistFailurePolicy,
    recovered_message: Option<Message>,
}

#[derive(JsonMessage, Serialize, Deserialize)]
#[result("()")]
struct Message;

impl PersistentActor for TestActor {
    fn configure(types: &mut JournalTypes<Self>) {
        types.message::<Message>("Message");
    }

    fn recovery_failure_policy(&self) -> RecoveryFailurePolicy {
        self.recovery_policy
    }

    fn persist_failure_policy(&self) -> PersistFailurePolicy {
        self.persist_policy
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
        if self.persist(&message, ctx).await.is_ok() {
            info!("received message");
        }
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
pub async fn test_persistent_actor_recovery_failure_retry_until_success() {
    util::create_trace_logger();

    let persistence = Arc::new(MockPersistence::default());
    let provider = Provider(persistence.clone());
    let system = ActorSystem::new().to_persistent(Persistence::from(provider));

    persistence.set_next_err(MockErr.into());
    persistence.set_next_err(MockErr.into());
    persistence.set_next_err(MockErr.into());
    persistence.set_next_err(MockErr.into());
    persistence.set_next_err(MockErr.into());

    let _actor = TestActor {
        recovery_policy: RecoveryFailurePolicy::Retry(Retry::UntilSuccess { delay: None }),
        persist_policy: Default::default(),
        recovered_message: None,
    }
    .into_actor(Some("TestActor".to_actor_id()), &system)
    .await
    .unwrap();

    info!("actor created")
}

#[tokio::test]
pub async fn test_persistent_actor_recovery_failure_stop_actor() {
    util::create_trace_logger();

    let persistence = Arc::new(MockPersistence::default());
    let provider = Provider(persistence.clone());
    let system = ActorSystem::new().to_persistent(Persistence::from(provider));

    persistence.set_next_err(MockErr.into());

    let actor = TestActor {
        recovery_policy: RecoveryFailurePolicy::StopActor,
        persist_policy: Default::default(),
        recovered_message: None,
    }
    .into_actor(Some("TestActor".to_actor_id()), &system)
    .await;

    assert_eq!(actor.unwrap_err(), ActorRefErr::ActorStartFailed)
}

#[tokio::test]
pub async fn test_persistent_actor_persist_failure_panic() {
    util::create_trace_logger();

    let persistence = Arc::new(MockPersistence::default());
    let provider = Provider(persistence.clone());
    let system = ActorSystem::new().to_persistent(Persistence::from(provider));

    let actor = TestActor {
        recovery_policy: Default::default(),
        persist_policy: PersistFailurePolicy::Panic,
        recovered_message: None,
    }
    .into_actor(Some("TestActor".to_actor_id()), &system)
    .await;

    let actor = actor.unwrap();
    assert!(actor.status().await.is_ok());

    persistence.set_next_err(MockErr.into());

    let result = actor.send(Message).await;
    assert!(result.is_err());
}

#[tokio::test]
pub async fn test_persistent_actor_persist_failure_retry_until_success() {
    util::create_trace_logger();

    let persistence = Arc::new(MockPersistence::default());
    let provider = Provider(persistence.clone());
    let system = ActorSystem::new().to_persistent(Persistence::from(provider));

    let actor = TestActor {
        recovery_policy: Default::default(),
        persist_policy: PersistFailurePolicy::Retry(Retry::UntilSuccess { delay: None }),
        recovered_message: None,
    }
    .into_actor(Some("TestActor".to_actor_id()), &system)
    .await;

    let actor = actor.unwrap();
    assert!(actor.status().await.is_ok());

    persistence.set_next_err(MockErr.into());
    persistence.set_next_err(MockErr.into());
    persistence.set_next_err(MockErr.into());
    persistence.set_next_err(MockErr.into());
    persistence.set_next_err(MockErr.into());

    let result = actor.send(Message).await;
    assert!(result.is_ok());
}

impl MockPersistence {
    pub fn set_next_err(&self, err: anyhow::Error) {
        let mut state = self.state.lock();
        state.next_errors.push_back(err);
    }
}

#[async_trait]
impl JournalStorage for MockPersistence {
    async fn write_snapshot(
        &self,
        _persistence_id: &str,
        _entry: JournalEntry,
    ) -> anyhow::Result<()> {
        let mut state = self.state.lock();
        if let Some(error) = state.next_errors.pop_front() {
            Err(error)
        } else {
            Ok(())
        }
    }

    async fn write_message(
        &self,
        _persistence_id: &str,
        _entry: JournalEntry,
    ) -> anyhow::Result<()> {
        let mut state = self.state.lock();
        if let Some(error) = state.next_errors.pop_front() {
            Err(error)
        } else {
            Ok(())
        }
    }

    async fn write_message_batch(
        &self,
        persistent_id: &str,
        entries: Vec<JournalEntry>,
    ) -> anyhow::Result<()> {
        todo!()
    }

    async fn read_latest_snapshot(
        &self,
        _persistence_id: &str,
    ) -> anyhow::Result<Option<JournalEntry>> {
        let mut state = self.state.lock();
        if let Some(error) = state.next_errors.pop_front() {
            Err(error)
        } else {
            Ok(state.snapshot.clone())
        }
    }

    async fn read_latest_messages(
        &self,
        _persistence_id: &str,
        _from_sequence: i64,
    ) -> anyhow::Result<Option<Vec<JournalEntry>>> {
        let mut state = self.state.lock();
        if let Some(error) = state.next_errors.pop_front() {
            Err(error)
        } else {
            Ok(state.messages.clone())
        }
    }

    async fn read_message(
        &self,
        persistence_id: &str,
        sequence_id: i64,
    ) -> anyhow::Result<Option<JournalEntry>> {
        todo!()
    }

    async fn read_messages(
        &self,
        persistence_id: &str,
        from_sequence: i64,
        to_sequence: i64,
    ) -> anyhow::Result<Option<Vec<JournalEntry>>> {
        todo!()
    }

    async fn delete_messages_to(
        &self,
        persistence_id: &str,
        to_sequence: i64,
    ) -> anyhow::Result<()> {
        todo!()
    }

    async fn delete_all(&self, _persistence_id: &str) -> anyhow::Result<()> {
        Ok(())
    }
}
