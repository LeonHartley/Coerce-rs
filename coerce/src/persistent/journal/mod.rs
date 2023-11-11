pub mod provider;
pub mod snapshot;
pub mod storage;
pub mod types;

use crate::actor::context::ActorContext;
use crate::actor::message::{Message, MessageUnwrapErr, MessageWrapErr};
use crate::persistent::journal::snapshot::Snapshot;
use crate::persistent::journal::storage::{JournalEntry, JournalStorageRef};
use crate::persistent::journal::types::{init_journal_types, JournalTypes};
use crate::persistent::{PersistentActor, Recover, RecoverSnapshot};

use crate::actor::metrics::ActorMetrics;
use crate::actor::Actor;
use crate::persistent::batch::EventBatch;
use crate::persistent::provider::StorageOptionsRef;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::marker::PhantomData;
use std::ops::Range;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::timeout;

pub mod proto;

pub struct Journal<A: PersistentActor> {
    persistence_id: String,
    last_sequence_id: i64,
    last_snapshot_sequence_id: Option<i64>,
    storage: JournalStorageRef,
    options: Option<StorageOptionsRef>,
    types: Arc<JournalTypes<A>>,
}

impl<A: PersistentActor> Journal<A> {
    pub fn new(
        persistence_id: String,
        storage: JournalStorageRef,
        options: Option<StorageOptionsRef>,
    ) -> Self {
        let last_sequence_id = 0;
        let last_snapshot_sequence_id = None;
        let types = init_journal_types::<A>();
        Self {
            persistence_id,
            last_sequence_id,
            last_snapshot_sequence_id,
            storage,
            types,
            options,
        }
    }

    pub fn persistence_id(&self) -> &str {
        &self.persistence_id
    }

    pub fn get_types(&self) -> Arc<JournalTypes<A>> {
        self.types.clone()
    }

    pub fn last_sequence_id(&self) -> i64 {
        self.last_sequence_id
    }
}

type RecoveryHandlerRef<A> = Arc<dyn RecoveryHandler<A>>;

#[derive(Debug)]
pub enum RecoveryErr {
    MessageDeserialisation {
        error: MessageUnwrapErr,
        message_type: &'static str,
        actor_type: &'static str,
        message_sequence_id: i64,
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
                message_sequence_id,
            } => {
                write!(f, "Message deserialisation error (message_type={message_type}, actor_type={actor_type}, sequence_id={sequence_id}) deserialisation error: {error}",
                       error = error,
                       message_type = message_type,
                       actor_type = actor_type,
                       sequence_id = message_sequence_id
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
        sequence_id: i64,
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
        self.handler
            .recover(actor, self.sequence, self.bytes, ctx)
            .await
    }
}

#[derive(Debug)]
pub enum PersistErr {
    Storage(anyhow::Error),
    Serialisation(MessageWrapErr),
    ActorStopping(Box<PersistErr>),
    NotConfigured(),
}

impl Display for PersistErr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "({:?})", &self)
    }
}

impl Error for PersistErr {}

type BytesRef = Arc<Vec<u8>>;

#[derive(Clone, Debug)]
pub struct ReadMessages<'a> {
    pub persistence_id: Option<&'a str>,
    pub read: Read,
}

impl<'a> ReadMessages<'a> {
    pub fn message(persistence_id: Option<&'a str>, sequence_id: i64) -> Self {
        Self {
            persistence_id,
            read: Read::Message { sequence_id },
        }
    }

    pub fn range(persistence_id: Option<&'a str>, range: Range<i64>) -> Self {
        Self {
            persistence_id,
            read: Read::Range(range),
        }
    }
}

#[derive(Clone, Debug)]
pub enum Read {
    Message { sequence_id: i64 },
    Range(Range<i64>),
}

impl<A: PersistentActor> Journal<A> {
    pub async fn persist_batch(&mut self, batch: &EventBatch<A>) -> Result<(), PersistErr> {
        let mut sequence_id = self.last_sequence_id;
        let batch = batch
            .entries()
            .iter()
            .map(|e| {
                sequence_id += 1;
                JournalEntry {
                    sequence: sequence_id,
                    payload_type: e.payload_type.clone(),
                    bytes: e.bytes.clone(),
                }
            })
            .collect();

        let res = self
            .storage
            .write_message_batch(&self.persistence_id, batch)
            .await
            .map_err(|e| PersistErr::Storage(e));

        if res.is_ok() {
            self.last_sequence_id = sequence_id;
        }

        res
    }

    pub async fn read_messages(
        &mut self,
        args: ReadMessages<'_>,
    ) -> anyhow::Result<Option<Vec<JournalEntry>>> {
        let persistence_id = args.persistence_id.unwrap_or(self.persistence_id.as_ref());
        match args.read {
            Read::Message { sequence_id } => Ok(self
                .storage
                .read_message(persistence_id, sequence_id)
                .await?
                .map(|m| vec![m])),

            Read::Range(range) => Ok(self
                .storage
                .read_messages(persistence_id, range.start, range.end)
                .await?),
        }
    }

    pub async fn persist_message<M: Message>(&mut self, bytes: BytesRef) -> Result<(), PersistErr>
    where
        A: Recover<M>,
    {
        trace!(
            persistence_id = &self.persistence_id,
            message_type = M::type_name(),
            "persisting message"
        );

        let payload_type = self
            .types
            .message_type_mapping::<M>()
            .expect("message type not configured");

        self.storage
            .write_message(
                &self.persistence_id,
                JournalEntry {
                    sequence: self.last_sequence_id,
                    payload_type: payload_type.clone(),
                    bytes,
                },
            )
            .await?;

        debug!(
            persistence_id = &self.persistence_id,
            message_type = M::type_name(),
            "persisted message"
        );

        self.last_sequence_id += 1;
        Ok(())
    }

    pub async fn persist_snapshot<S: Snapshot>(
        &mut self,
        bytes: BytesRef,
    ) -> Result<(), PersistErr> {
        debug!(persistence_id = &self.persistence_id, "persisting snapshot");

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
        self.last_snapshot_sequence_id = Some(sequence);
        Ok(())
    }

    pub async fn recover_snapshot(&mut self) -> Result<Option<RecoveredPayload<A>>, anyhow::Error> {
        let read_snapshot = self.storage.read_latest_snapshot(&self.persistence_id);

        let snapshot = match self.options.as_ref().and_then(|o| o.snapshot_read_timeout) {
            Some(timeout_duration) => timeout(timeout_duration, read_snapshot).await?,
            None => read_snapshot.await,
        }?;

        if let Some(raw_snapshot) = snapshot {
            let handler = self
                .types
                .recoverable_snapshots()
                .get(raw_snapshot.payload_type.as_ref());

            let sequence = raw_snapshot.sequence;
            let bytes =
                Arc::try_unwrap(raw_snapshot.bytes).map_or_else(|e| e.as_ref().clone(), |s| s);

            self.last_sequence_id = sequence;
            self.last_snapshot_sequence_id = Some(sequence);

            debug!(
                persistence_id = &self.persistence_id,
                last_sequence_id = &self.last_sequence_id,
                snapshot_type = raw_snapshot.payload_type.as_ref(),
                "snapshot recovered"
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

        let read_messages = self
            .storage
            .read_latest_messages(&self.persistence_id, self.last_sequence_id);

        let messages = match self.options.as_ref().and_then(|o| o.journal_read_timeout) {
            Some(timeout_duration) => timeout(timeout_duration, read_messages).await?,
            None => read_messages.await,
        }?;

        if let Some(messages) = messages {
            let starting_sequence = self.last_sequence_id;
            let mut recoverable_messages = vec![];
            for entry in messages {
                self.last_sequence_id = entry.sequence;

                if let Some(handler) = self
                    .types
                    .recoverable_messages()
                    .get(entry.payload_type.as_ref())
                {
                    trace!(
                        persistence_id = &self.persistence_id,
                        sequence_id = &self.last_sequence_id,
                        starting_sequence = starting_sequence,
                        message_type = &entry.payload_type.as_ref(),
                        "message recovered"
                    );

                    let bytes =
                        Arc::try_unwrap(entry.bytes).map_or_else(|e| e.as_ref().clone(), |s| s);

                    recoverable_messages.push(RecoveredPayload {
                        bytes,
                        sequence: entry.sequence,
                        handler: handler.clone(),
                    })
                } else {
                    error!(
                        persistence_id = &self.persistence_id,
                        payload_type = entry.payload_type.as_ref(),
                        "recovered message but actor is not configured to process it"
                    );
                    // TODO: this should fail recovery
                }
            }

            trace!(
                persistence_id = &self.persistence_id,
                last_sequence_id = &self.last_sequence_id,
                "recovery complete"
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

    pub async fn clear(&mut self) -> bool {
        self.storage.delete_all(&self.persistence_id).await.is_ok()
    }

    pub async fn clear_old_messages(&mut self) -> bool {
        if let Some(snapshot_sequence_id) = self.last_snapshot_sequence_id {
            self.storage
                .delete_messages_to(&self.persistence_id, snapshot_sequence_id)
                .await
                .is_ok()
        } else {
            // no messages to delete
            false
        }
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
        sequence_id: i64,
        bytes: Vec<u8>,
        ctx: &mut ActorContext,
    ) -> Result<(), RecoveryErr> {
        let message =
            M::from_bytes(bytes).map_err(|error| RecoveryErr::MessageDeserialisation {
                error,
                message_type: M::type_name(),
                actor_type: A::type_name(),
                message_sequence_id: sequence_id,
            })?;

        let start = Instant::now();

        Recover::recover(actor, message, ctx).await;
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
        _sequence_id: i64,
        bytes: Vec<u8>,
        ctx: &mut ActorContext,
    ) -> Result<(), RecoveryErr> {
        RecoverSnapshot::recover(
            actor,
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
