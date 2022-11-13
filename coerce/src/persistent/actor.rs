use crate::actor::context::{ActorContext, ActorStatus};
use crate::actor::message::Message;
use crate::actor::system::ActorSystem;
use crate::actor::{Actor, BoxedActorRef};

use crate::persistent::failure::{should_retry, PersistFailurePolicy, RecoveryFailurePolicy};
use crate::persistent::journal::snapshot::Snapshot;
use crate::persistent::journal::types::JournalTypes;
use crate::persistent::journal::{PersistErr, RecoveredPayload, RecoveryErr};
use crate::persistent::recovery::{ActorRecovery, RecoveryResult};

use crate::persistent::RecoveredJournal;
use std::error::Error;
use std::fmt;
use std::fmt::{Debug, Display, Formatter};
use std::ops::Add;
use std::sync::Arc;
use std::time::Duration;

#[async_trait]
pub trait Recover<M: Message> {
    async fn recover(&mut self, message: M, ctx: &mut ActorContext);
}

#[async_trait]
pub trait RecoverSnapshot<S: Snapshot> {
    async fn recover(&mut self, snapshot: S, ctx: &mut ActorContext);
}

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

    async fn recover_journal(
        &mut self,
        persistence_key: String,
        ctx: &mut ActorContext,
    ) -> RecoveryResult<Self> {
        ActorRecovery::recover_journal(self, Some(persistence_key), ctx).await
    }

    fn recovery_failure_policy(&self) -> RecoveryFailurePolicy {
        RecoveryFailurePolicy::default()
    }

    fn persist_failure_policy(&self) -> PersistFailurePolicy {
        PersistFailurePolicy::default()
    }

    async fn on_recovery_err(&mut self, _err: RecoveryErr, _ctx: &mut ActorContext) {}

    async fn on_recovery_failed(&mut self, _ctx: &mut ActorContext) {}
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
        system: Option<ActorSystem>,
        status: ActorStatus,
        boxed_ref: BoxedActorRef,
    ) -> ActorContext
    where
        Self: Sized,
    {
        ActorContext::new(system, status, boxed_ref).with_persistence()
    }

    async fn started(&mut self, ctx: &mut ActorContext) {
        trace!("persistent actor starting, loading journal");
        self.pre_recovery(ctx).await;

        let persistence_key = self.persistence_key(ctx);
        let (snapshot, messages) = {
            let journal = self.recover_journal(persistence_key.clone(), ctx).await;
            match journal {
                RecoveryResult::Recovered(journal) => (journal.snapshot, journal.messages),
                RecoveryResult::Failed => {
                    trace!("recovery failed, ctx_status={:?}", ctx.get_status());
                    self.on_recovery_failed(ctx).await;
                    return;
                }
            }
        };

        trace!(
            "persistent actor ({}) recovered {} snapshot(s) and {} message(s)",
            &persistence_key,
            if snapshot.is_some() { 1 } else { 0 },
            messages.as_ref().map_or(0, |m| m.len()),
        );

        if let Some(snapshot) = snapshot {
            if let Err(e) = snapshot.recover(self, ctx).await {
                // non-transient error, i.e serialisation errors should not be retried, we should just exit here.
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
                    // non-transient error, i.e serialisation errors should not be retried, we should just exit here.
                    error!("Error while attempting to recover from a message, error={error}, actor_id={actor_id}, persistence_key={persistence_key}",
                        error = &e,
                        actor_id = ctx.id(),
                        persistence_key = &persistence_key
                    );

                    // non-transient error, i.e serialisation errors should not be retried, we should just exit here.
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
}
