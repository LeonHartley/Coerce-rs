pub mod provider;
pub mod snapshot;
pub mod storage;
pub mod types;

use crate::actor::context::ActorContext;
use crate::actor::message::{EnvelopeType, Handler, Message};
use crate::persistent::journal::snapshot::{JournalPayload, Snapshot};
use crate::persistent::journal::storage::{JournalEntry, JournalStorage, JournalStorageRef};
use crate::persistent::journal::types::{init_journal_types, JournalTypes};
use crate::persistent::{PersistentActor, Recover, RecoverSnapshot};

use slog::Logger;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

pub struct Journal<A: PersistentActor> {
    persistence_id: String,
    last_sequence_id: i64,
    storage: JournalStorageRef,
    types: Arc<JournalTypes<A>>,
    log: Logger,
}

impl<A: PersistentActor> Journal<A> {
    pub async fn new(persistence_id: String, storage: JournalStorageRef, log: Logger) -> Self {
        let last_sequence_id = 0;
        let types = init_journal_types::<A>().await;
        Self {
            persistence_id,
            last_sequence_id,
            storage,
            types,
            log,
        }
    }
}

type RecoveryHandlerRef<A: PersistentActor> = Arc<dyn RecoveryHandler<A>>;

#[async_trait]
pub trait RecoveryHandler<A>: 'static + Send + Sync {
    async fn recover(&self, actor: &mut A, bytes: Vec<u8>, ctx: &mut ActorContext);
}

pub struct MessageRecoveryHandler<A: PersistentActor, M: Message>(PhantomData<A>, PhantomData<M>);

pub struct SnapshotRecoveryHandler<A: PersistentActor, S: Snapshot>(PhantomData<A>, PhantomData<S>);

impl<A: PersistentActor, M: Message> MessageRecoveryHandler<A, M> {
    pub fn new() -> Self {
        MessageRecoveryHandler(PhantomData, PhantomData)
    }
}

impl<A: PersistentActor, S: Snapshot> SnapshotRecoveryHandler<A, S> {
    pub fn new() -> Self {
        SnapshotRecoveryHandler(PhantomData, PhantomData)
    }
}

pub struct RecoveredPayload<A: PersistentActor> {
    bytes: Vec<u8>,
    sequence: i64,
    handler: RecoveryHandlerRef<A>,
}

impl<A: PersistentActor> RecoveredPayload<A> {
    pub async fn recover(self, actor: &mut A, ctx: &mut ActorContext) {
        self.handler.recover(actor, self.bytes, ctx).await
    }
}

#[derive(Debug)]
pub enum PersistErr {
    Storage(Box<dyn Error>),
}

impl Display for PersistErr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "({:?})", &self)
    }
}

impl Error for PersistErr {}

impl<A: PersistentActor> Journal<A> {
    pub async fn persist_message<M: Message>(&mut self, message: &M) -> Result<(), PersistErr>
    where
        A: Recover<M>,
    {
        info!(
            &self.log,
            "persisting message, persistence_id={}", &self.persistence_id
        );

        let payload_type = self
            .types
            .message_type_mapping::<M>()
            .expect("message type not configured");

        let bytes = message
            .as_remote_envelope()
            .expect("cannot serialise message")
            .into_bytes();

        let sequence = self.last_sequence_id + 1;

        self.storage
            .write_message(
                &self.persistence_id,
                JournalEntry {
                    sequence,
                    payload_type,
                    bytes,
                },
            )
            .await;

        self.last_sequence_id = sequence;
        Ok(())
    }

    pub async fn persist_snapshot<S: Snapshot>(&mut self, snapshot: S) -> Result<(), PersistErr> {
        info!(
            &self.log,
            "persisting snapshot, persistence_id={}", &self.persistence_id
        );

        Ok(())
    }

    pub async fn recover_snapshot(&mut self) -> Option<RecoveredPayload<A>> {
        if let Some(raw_snapshot) = self
            .storage
            .read_latest_snapshot(&self.persistence_id)
            .await
        {
            let handler = self
                .types
                .recoverable_snapshots()
                .get(&raw_snapshot.payload_type);

            let sequence = raw_snapshot.sequence;
            let bytes = raw_snapshot.bytes;

            self.last_sequence_id = sequence;

            trace!(
                &self.log,
                "snapshot recovered (persistence_id={}), last sequence={}, type={}",
                &self.persistence_id,
                &self.last_sequence_id,
                &raw_snapshot.payload_type
            );

            handler.map(|handler| RecoveredPayload {
                bytes,
                sequence,
                handler: handler.clone(),
            })
        } else {
            None
        }
    }

    pub async fn recover_messages(&mut self) -> Option<Vec<RecoveredPayload<A>>> {
        if let Some(messages) = self
            .storage
            .read_latest_messages(&self.persistence_id, self.last_sequence_id)
            .await
        {
            let starting_sequence = self.last_sequence_id;
            let mut recoverable_messages = vec![];
            for entry in messages {
                self.last_sequence_id = entry.sequence;

                if let Some(handler) = self.types.recoverable_messages().get(&entry.payload_type) {
                    trace!(
                        &self.log,
                        "message recovered (persistence_id={}), sequence={}, starting_sequence={} type={}",
                        &self.persistence_id,
                        &self.last_sequence_id,
                        starting_sequence,
                        &entry.payload_type
                    );

                    recoverable_messages.push(RecoveredPayload {
                        bytes: entry.bytes,
                        sequence: entry.sequence,
                        handler: handler.clone(),
                    })
                } else {
                    warn!(&self.log, "persistence_id={} recovered message (type={}) but actor is not configured to process it", &entry.payload_type, &self.persistence_id);
                }
            }

            trace!(
                &self.log,
                "recovery complete, last_sequence_id={}",
                &self.last_sequence_id
            );
            Some(recoverable_messages)
        } else {
            None
        }
    }

    pub async fn delete_messages(&mut self) {}
}

#[async_trait]
impl<A: PersistentActor, M: Message> RecoveryHandler<A> for MessageRecoveryHandler<A, M>
where
    A: Recover<M>,
{
    async fn recover(&self, actor: &mut A, bytes: Vec<u8>, ctx: &mut ActorContext) {
        let message = M::from_remote_envelope(bytes);
        if let Ok(message) = message {
            actor.recover(message, ctx).await;
        } else {
            // todo: log serialisation error / fail the recovery
        }
    }
}

#[async_trait]
impl<A: PersistentActor, S: Snapshot> RecoveryHandler<A> for SnapshotRecoveryHandler<A, S>
where
    A: RecoverSnapshot<S>,
{
    async fn recover(&self, actor: &mut A, bytes: Vec<u8>, ctx: &mut ActorContext) {
        let message = S::from_remote_envelope(bytes);
        if let Ok(message) = message {
            actor.recover(message, ctx).await;
        } else {
            // todo: log serialisation error / fail the recovery
        }
    }
}
