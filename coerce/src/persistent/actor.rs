use crate::actor::context::{ActorContext, ActorStatus};
use crate::actor::message::Message;
use crate::actor::system::ActorSystem;
use crate::actor::{Actor, ActorId, BoxedActorRef};

use crate::persistent::failure::{should_retry, PersistFailurePolicy, RecoveryFailurePolicy};
use crate::persistent::journal::snapshot::Snapshot;
use crate::persistent::journal::types::JournalTypes;
use crate::persistent::journal::{PersistErr, RecoveryErr};
use crate::persistent::recovery::{ActorRecovery, Recovery};

use crate::persistent::batch::EventBatch;
use crate::persistent::storage::JournalEntry;
use crate::persistent::ReadMessages;
use std::sync::Arc;

#[async_trait]
pub trait PersistentActor: 'static + Sized + Send + Sync {
    fn persistence_key(&self, ctx: &ActorContext) -> String {
        ctx.id().to_string()
    }

    fn configure(types: &mut JournalTypes<Self>);

    async fn pre_recovery(&mut self, _ctx: &mut ActorContext) {}

    async fn post_recovery(&mut self, _ctx: &mut ActorContext) {}

    async fn stopped(&mut self, _ctx: &mut ActorContext) {}

    async fn persist<M: Message>(
        &self,
        message: &M,
        ctx: &mut ActorContext,
    ) -> Result<(), PersistErr>
    where
        Self: Recover<M>,
    {
        let message_bytes = message.as_bytes();
        match message_bytes {
            Ok(bytes) => {
                let mut attempts = 1;
                let bytes = Arc::new(bytes);
                loop {
                    let result = ctx
                        .persistence_mut()
                        .journal_mut::<Self>()
                        .persist_message::<M>(bytes.clone())
                        .await;

                    if let Some(res) = check(result, &mut attempts, self, ctx).await {
                        return res;
                    }
                }
            }

            Err(e) => return Err(PersistErr::Serialisation(e)),
        }
    }

    async fn persist_batch(
        &self,
        batch: EventBatch<Self>,
        ctx: &mut ActorContext,
    ) -> Result<(), PersistErr> {
        if batch.entries().is_empty() {
            return Ok(());
        }
        let mut attempts = 1;
        loop {
            let result = ctx
                .persistence_mut()
                .journal_mut::<Self>()
                .persist_batch(&batch)
                .await;

            if let Some(res) = check(result, &mut attempts, self, ctx).await {
                return res;
            }
        }
    }

    async fn snapshot<S: Snapshot>(
        &self,
        snapshot: S,
        ctx: &mut ActorContext,
    ) -> Result<(), PersistErr>
    where
        Self: RecoverSnapshot<S>,
    {
        let snapshot_bytes = snapshot.into_remote_envelope();
        match snapshot_bytes {
            Ok(bytes) => {
                let bytes = Arc::new(bytes.into_bytes());

                let mut attempts = 1;
                loop {
                    let result = ctx
                        .persistence_mut()
                        .journal_mut::<Self>()
                        .persist_snapshot::<S>(bytes.clone())
                        .await;

                    if let Some(res) = check(result, &mut attempts, self, ctx).await {
                        return res;
                    }
                }
            }

            Err(e) => return Err(PersistErr::Serialisation(e)),
        }
    }

    async fn recover(&mut self, persistence_key: String, ctx: &mut ActorContext) -> Recovery<Self> {
        ActorRecovery::recover_journal(self, Some(persistence_key), ctx).await
    }

    fn last_sequence_id(&self, ctx: &mut ActorContext) -> i64 {
        ctx.persistence_mut()
            .journal_mut::<Self>()
            .last_sequence_id()
    }

    async fn read_messages<'a>(
        &self,
        args: ReadMessages<'a>,
        ctx: &mut ActorContext,
    ) -> anyhow::Result<Option<Vec<JournalEntry>>> {
        ctx.persistence_mut()
            .journal_mut::<Self>()
            .read_messages(args)
            .await
    }

    async fn clear_old_messages(&mut self, ctx: &mut ActorContext) -> bool {
        ctx.persistence_mut()
            .journal_mut::<Self>()
            .clear_old_messages()
            .await
    }

    fn recovery_failure_policy(&self) -> RecoveryFailurePolicy {
        RecoveryFailurePolicy::default()
    }

    fn persist_failure_policy(&self) -> PersistFailurePolicy {
        PersistFailurePolicy::default()
    }

    fn event_batch(&self, ctx: &ActorContext) -> EventBatch<Self> {
        EventBatch::create(ctx)
    }

    async fn on_recovery_err(&mut self, _err: RecoveryErr, _ctx: &mut ActorContext) {}

    async fn on_recovery_failed(&mut self, _ctx: &mut ActorContext) {}

    async fn on_child_stopped(&mut self, _id: &ActorId, _ctx: &mut ActorContext) {}
}

#[async_trait]
pub trait Recover<M: Message> {
    async fn recover(&mut self, message: M, ctx: &mut ActorContext);
}

#[async_trait]
pub trait RecoverSnapshot<S: Snapshot> {
    async fn recover(&mut self, snapshot: S, ctx: &mut ActorContext);
}

async fn check<A: PersistentActor>(
    result: Result<(), PersistErr>,
    attempts: &mut usize,
    actor: &A,
    ctx: &mut ActorContext,
) -> Option<Result<(), PersistErr>> {
    match result {
        Ok(res) => return Some(Ok(res)),
        Err(e) => {
            let failure_policy = actor.persist_failure_policy();

            error!("persist failed, error={error}, actor_id={actor_id}, policy={failure_policy}, attempt={attempt}",
                error = e,
                actor_id = ctx.id(),
                failure_policy = &failure_policy,
                attempt = attempts
            );

            match failure_policy {
                PersistFailurePolicy::Retry(retry_policy) => {
                    if !should_retry(ctx, &attempts, retry_policy).await {
                        return Some(Err(e));
                    }
                }

                PersistFailurePolicy::ReturnErr => {
                    return Some(Err(e));
                }

                PersistFailurePolicy::StopActor => {
                    ctx.stop(None);
                    return Some(Err(PersistErr::ActorStopping(Box::new(e))));
                }

                PersistFailurePolicy::Panic => {
                    panic!("persist failed");
                }
            }

            *attempts += 1;
            None
        }
    }
}

#[async_trait]
impl<A: 'static + PersistentActor + Send + Sync> Actor for A
where
    A: Sized,
{
    fn new_context(
        &self,
        system: Option<ActorSystem>,
        status: ActorStatus,
        boxed_ref: BoxedActorRef,
    ) -> ActorContext {
        ActorContext::new(system, status, boxed_ref, Self::DEFAULT_TAGS).with_persistence()
    }

    async fn started(&mut self, ctx: &mut ActorContext) {
        trace!("persistent actor starting, loading journal");

        self.pre_recovery(ctx).await;

        let persistence_key = self.persistence_key(ctx);
        let (snapshot, messages) = {
            let journal = self.recover(persistence_key.clone(), ctx).await;
            match journal {
                Recovery::Recovered(journal) => {
                    trace!(
                        "persistent actor ({}) recovered {} snapshot(s) and {} message(s)",
                        &persistence_key,
                        if journal.snapshot.is_some() { 1 } else { 0 },
                        journal.messages.as_ref().map_or(0, |m| m.len()),
                    );

                    (journal.snapshot, journal.messages)
                }
                Recovery::Disabled => {
                    trace!(
                        "persistent actor ({}) disabled message recovery",
                        &persistence_key,
                    );

                    (None, None)
                }
                Recovery::Failed => {
                    trace!("recovery failed, ctx_status={:?}", ctx.get_status());
                    self.on_recovery_failed(ctx).await;
                    return;
                }
            }
        };

        if let Some(snapshot) = snapshot {
            if let Err(e) = snapshot.recover(self, ctx).await {
                error!("Error while attempting to recover from a snapshot, error={error}, actor_id={actor_id}, persistence_key={persistence_key}",
                    error = &e,
                    actor_id = ctx.id(),
                    persistence_key = &persistence_key
                );

                self.on_recovery_err(e, ctx).await;
                ctx.stop(None);
                return;
            }
        }

        if let Some(messages) = messages {
            for message in messages {
                if let Err(e) = message.recover(self, ctx).await {
                    error!("Error while attempting to recover from a message, error={error}, actor_id={actor_id}, persistence_key={persistence_key}",
                        error = &e,
                        actor_id = ctx.id(),
                        persistence_key = &persistence_key
                    );

                    self.on_recovery_err(e, ctx).await;
                    ctx.stop(None);
                    return;
                }
            }
        }

        self.post_recovery(ctx).await;
    }

    async fn stopped(&mut self, ctx: &mut ActorContext) {
        trace!("persistent actor stopped");
        self.stopped(ctx).await
    }

    async fn on_child_stopped(&mut self, id: &ActorId, ctx: &mut ActorContext) {
        self.on_child_stopped(id, ctx).await
    }
}
