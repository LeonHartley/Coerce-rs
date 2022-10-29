use crate::actor::message::Message;
use crate::persistent::journal::snapshot::Snapshot;
use crate::persistent::journal::{
    MessageRecoveryHandler, RecoveryHandlerRef, SnapshotRecoveryHandler,
};
use crate::persistent::{PersistentActor, RecoverSnapshot};
use std::any::Any;
use std::any::TypeId;
use std::collections::HashMap;
use std::sync::Arc;

lazy_static! {
    static ref JOURNAL_TYPE_CACHE: parking_lot::Mutex<HashMap<TypeId, Arc<dyn Any + Send + Sync>>> =
        parking_lot::Mutex::new(HashMap::new());
}

pub struct JournalTypes<A: PersistentActor> {
    message_type_map: HashMap<TypeId, String>,
    snapshot_type_map: HashMap<TypeId, String>,
    recoverable_messages: HashMap<String, RecoveryHandlerRef<A>>,
    recoverable_snapshots: HashMap<String, RecoveryHandlerRef<A>>,
}

impl<A: PersistentActor> Default for JournalTypes<A> {
    fn default() -> Self {
        let message_type_map = HashMap::new();
        let snapshot_type_map = HashMap::new();
        let recoverable_messages = HashMap::new();
        let recoverable_snapshots = HashMap::new();
        JournalTypes {
            message_type_map,
            snapshot_type_map,
            recoverable_messages,
            recoverable_snapshots,
        }
    }
}

impl<A: PersistentActor> JournalTypes<A> {
    pub fn message<M: Message>(&mut self, identifier: &str) -> &mut Self
    where
        A: Recover<M>,
    {
        self.recoverable_messages.insert(
            identifier.to_string(),
            Arc::new(MessageRecoveryHandler::new()),
        );

        self.message_type_map
            .insert(TypeId::of::<M>(), identifier.to_string());

        self
    }

    pub fn snapshot<S: Snapshot>(&mut self, identifier: &str) -> &mut Self
    where
        A: RecoverSnapshot<S>,
    {
        self.recoverable_snapshots.insert(
            identifier.to_string(),
            Arc::new(SnapshotRecoveryHandler::new()),
        );

        self.snapshot_type_map
            .insert(TypeId::of::<S>(), identifier.to_string());

        self
    }

    pub fn snapshot_type_mapping<S: Snapshot>(&self) -> Option<String> {
        self.snapshot_type_map.get(&TypeId::of::<S>()).cloned()
    }

    pub fn message_type_mapping<M: Message>(&self) -> Option<String> {
        self.message_type_map.get(&TypeId::of::<M>()).cloned()
    }

    pub fn recoverable_snapshots(&self) -> &HashMap<String, RecoveryHandlerRef<A>> {
        &self.recoverable_snapshots
    }

    pub fn recoverable_messages(&self) -> &HashMap<String, RecoveryHandlerRef<A>> {
        &self.recoverable_messages
    }
}

pub(crate) fn init_journal_types<A: PersistentActor>() -> Arc<JournalTypes<A>> {
    let actor_type_id = TypeId::of::<A>();
    if let Some(types) = get_cached_types(&actor_type_id) {
        return types;
    }

    let mut types = JournalTypes::default();
    A::configure(&mut types);

    let types = Arc::new(types);
    JOURNAL_TYPE_CACHE
        .lock()
        .insert(actor_type_id, types.clone());

    types
}

fn get_cached_types<A: PersistentActor>(actor_type_id: &TypeId) -> Option<Arc<JournalTypes<A>>> {
    if let Some(journal_types) = JOURNAL_TYPE_CACHE.lock().get(&actor_type_id) {
        Some(journal_types.clone().downcast().unwrap())
    } else {
        None
    }
}
