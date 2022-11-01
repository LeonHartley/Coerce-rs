pub mod provider;
pub mod snapshot;
pub mod storage;
pub mod types;

use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message, MessageUnwrapErr, MessageWrapErr};
use crate::persistent::journal::snapshot::Snapshot;
use crate::persistent::journal::storage::{JournalEntry, JournalStorageRef};
use crate::persistent::journal::types::{init_journal_types, JournalTypes};
use crate::persistent::{PersistentActor, Recover, RecoverSnapshot};

use crate::actor::metrics::ActorMetrics;
use crate::actor::Actor;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub mod proto;

pub struct Journal<A: PersistentActor> {
    persistence_id: String,
    last_sequence_id: i64,
    storage: JournalStorageRef,
    types: Arc<JournalTypes<A>>,
}

impl<A: PersistentActor> Journal<A> {
    pub fn new(persistence_id: String, storage: JournalStorageRef) -> Self {
        let last_sequence_id = 0;
        let types = init_journal_types::<A>();
        Self {
            persistence_id,
            last_sequence_id,
            storage,
            types,
        }
    }
}

type RecoveryHandlerRef<A> = Arc<dyn RecoveryHandler<A>>;

#[derive(Debug)]
pub enum RecoveryErr {
    MessageDeserialisation {
        error: MessageUnwrapErr,
        message_type: &'static str,
        actor_type: &'static str,
    },

    SnapshotDeserialisation {
        error: MessageUnwrapErr,
        snapshot_type: &'static str,
        actor_type: &'static str,
    },

    Snapshot(anyhow::Error),
    Messages(anyhow::Error),
}

impl Display for RecoveryErr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self {
            RecoveryErr::MessageDeserialisation {
                error,
                message_type,
                actor_type,
            } => {
                write!(f, "Message deserialisation error, message_type={message_type}, actor_type={actor_type}, deserialisation error: {error}",
                       error = error,
                       message_type = message_type,
                       actor_type = actor_type
                )
            }

            RecoveryErr::SnapshotDeserialisation {
                error,
                snapshot_type,
                actor_type,
            } => {
                write!(f, "Snapshot deserialisation error, snapshot_type={snapshot_type}, actor_type={actor_type}, deserialisation error: {error}",
                       error = error,
                       snapshot_type = snapshot_type,
                       actor_type = actor_type
                )
            }

            RecoveryErr::Snapshot(e) => {
                write!(f, "Snapshot recovery error: {error}", error = e)
            }

            RecoveryErr::Messages(e) => {
                write!(f, "Message recovery error: {error}", error = e)
            }
        }
    }
}

impl Error for RecoveryErr {}

#[async_trait]
pub trait RecoveryHandler<A>: 'static + Send + Sync {
    async fn recover(
        &self,
        actor: &mut A,
        bytes: Vec<u8>,
        ctx: &mut ActorContext,
    ) -> Result<(), RecoveryErr>;
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
    pub async fn recover(self, actor: &mut A, ctx: &mut ActorContext) -> Result<(), RecoveryErr> {
        self.handler.recover(actor, self.bytes, ctx).await
    }
}

#[derive(Debug)]
pub enum PersistErr {
    Storage(anyhow::Error),
    Serialisation(MessageWrapErr),
    ActorStopping(Box<PersistErr>),
}

impl Display for PersistErr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "({:?})", &self)
    }
}

impl Error for PersistErr {}

type BytesRef = Arc<Vec<u8>>;

impl<A: PersistentActor> Journal<A> {
    pub async fn persist_message<M: Message>(&mut self, bytes: BytesRef) -> Result<(), PersistErr>
    where
        A: Recover<M>,
    {
        trace!(
            "persisting message, persistence_id={}, message_type={}",
            &self.persistence_id,
            M::type_name()
        );

        let payload_type = self
            .types
            .message_type_mapping::<M>()
            .expect("message type not configured");

        let sequence = self.last_sequence_id + 1;

        self.storage
            .write_message(
                &self.persistence_id,
                JournalEntry {
                    sequence,
                    payload_type: payload_type.clone(),
                    bytes,
                },
            )
            .await?;

        debug!(
            "persisted message, persistence_id={}, message_type={}",
            &self.persistence_id,
            M::type_name()
        );

        self.last_sequence_id = sequence;
        Ok(())
    }

    pub async fn persist_snapshot<S: Snapshot>(
        &mut self,
        bytes: BytesRef,
    ) -> Result<(), PersistErr> {
        info!(
            "persisting snapshot, persistence_id={}",
            &self.persistence_id
        );

        let payload_type = self
            .types
            .snapshot_type_mapping::<S>()
            .expect("snapshot type not configured");

        let sequence = self.last_sequence_id + 1;

        self.storage
            .write_snapshot(
                &self.persistence_id,
                JournalEntry {
                    sequence,
                    payload_type,
                    bytes,
                },
            )
            .await?;

        self.last_sequence_id = sequence;
        Ok(())
    }

    pub async fn recover_snapshot(&mut self) -> Result<Option<RecoveredPayload<A>>, anyhow::Error> {
        if let Some(raw_snapshot) = self
            .storage
            .read_latest_snapshot(&self.persistence_id)
            .await?
        {
            let handler = self
                .types
                .recoverable_snapshots()
                .get(&raw_snapshot.payload_type);

            let sequence = raw_snapshot.sequence;
            let bytes =
                Arc::try_unwrap(raw_snapshot.bytes).map_or_else(|e| e.as_ref().clone(), |s| s);

            self.last_sequence_id = sequence;

            debug!(
                "snapshot recovered (persistence_id={}), last sequence={}, type={}",
                &self.persistence_id, &self.last_sequence_id, &raw_snapshot.payload_type
            );

            Ok(handler.map(|handler| RecoveredPayload {
                bytes,
                sequence,
                handler: handler.clone(),
            }))
        } else {
            Ok(None)
        }
    }

    pub async fn recover_messages(
        &mut self,
    ) -> Result<Option<Vec<RecoveredPayload<A>>>, anyhow::Error> {
        // TODO: route journal recovery through a system that we can apply limiting to so we can only
        //       recover {n} entities at a time, so we don't end up bringing down
        //       the storage backend
        if let Some(messages) = self
            .storage
            .read_latest_messages(&self.persistence_id, self.last_sequence_id)
            .await?
        {
            let starting_sequence = self.last_sequence_id;
            let mut recoverable_messages = vec![];
            for entry in messages {
                self.last_sequence_id = entry.sequence;

                if let Some(handler) = self.types.recoverable_messages().get(&entry.payload_type) {
                    trace!(
                        "message recovered (persistence_id={}), sequence={}, starting_sequence={} type={}",
                        &self.persistence_id,
                        &self.last_sequence_id,
                        starting_sequence,
                        &entry.payload_type
                    );

                    let bytes =
                        Arc::try_unwrap(entry.bytes).map_or_else(|e| e.as_ref().clone(), |s| s);

                    recoverable_messages.push(RecoveredPayload {
                        bytes,
                        sequence: entry.sequence,
                        handler: handler.clone(),
                    })
                } else {
                    error!("persistence_id={} recovered message (type={}) but actor is not configured to process it",  &self.persistence_id, &entry.payload_type);
                    // TODO: this should fail recovery
                }
            }

            trace!(
                "recovery complete, last_sequence_id={}",
                &self.last_sequence_id
            );

            if recoverable_messages.len() == 0 {
                Ok(None)
            } else {
                Ok(Some(recoverable_messages))
            }
        } else {
            Ok(None)
        }
    }

    pub async fn delete_messages(&mut self) -> bool {
        let delete_result = self.storage.delete_all(&self.persistence_id).await;

        delete_result.is_ok()
    }
}

#[async_trait]
impl<A: PersistentActor, M: Message> RecoveryHandler<A> for MessageRecoveryHandler<A, M>
where
    A: Recover<M>,
{
    async fn recover(
        &self,
        actor: &mut A,
        bytes: Vec<u8>,
        ctx: &mut ActorContext,
    ) -> Result<(), RecoveryErr> {
        let message =
            M::from_bytes(bytes).map_err(|error| RecoveryErr::MessageDeserialisation {
                error,
                message_type: M::type_name(),
                actor_type: A::type_name(),
            })?;

        let start = Instant::now();

        actor.recover(message, ctx).await;
        let message_processing_took = start.elapsed();

        ActorMetrics::incr_messages_processed(
            A::type_name(),
            M::type_name(),
            Duration::default(),
            message_processing_took,
        );

        Ok(())
    }
}

#[async_trait]
impl<A: PersistentActor, S: Snapshot> RecoveryHandler<A> for SnapshotRecoveryHandler<A, S>
where
    A: RecoverSnapshot<S>,
{
    async fn recover(
        &self,
        actor: &mut A,
        bytes: Vec<u8>,
        ctx: &mut ActorContext,
    ) -> Result<(), RecoveryErr> {
        actor
            .recover(
                S::from_remote_envelope(bytes).map_err(|error| {
                    RecoveryErr::SnapshotDeserialisation {
                        error,
                        snapshot_type: S::type_name(),
                        actor_type: A::type_name(),
                    }
                })?,
                ctx,
            )
            .await;
        Ok(())
    }
}

impl From<anyhow::Error> for PersistErr {
    fn from(e: anyhow::Error) -> Self {
        Self::Storage(e)
    }
}
