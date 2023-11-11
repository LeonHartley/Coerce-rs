use crate::actor::context::ActorContext;
use crate::persistent::failure::{should_retry, RecoveryFailurePolicy};
use crate::persistent::journal::{RecoveredPayload, RecoveryErr};
use crate::persistent::PersistentActor;

#[async_trait]
pub trait ActorRecovery: PersistentActor {
    async fn recover_journal(
        &mut self,
        persistence_key: Option<String>,
        ctx: &mut ActorContext,
    ) -> Recovery<Self>;
}

pub enum Recovery<A: PersistentActor> {
    Recovered(RecoveredJournal<A>),
    Disabled,
    Failed,
}

#[async_trait]
impl<A: PersistentActor> ActorRecovery for A {
    async fn recover_journal(
        &mut self,
        persistence_key: Option<String>,
        ctx: &mut ActorContext,
    ) -> Recovery<Self> {
        let mut journal = None;
        let mut attempts = 1;

        let persistence_key = persistence_key.unwrap_or_else(|| self.persistence_key(ctx));

        loop {
            match load_journal::<Self>(persistence_key.clone(), ctx).await {
                Ok(loaded_journal) => {
                    journal = Some(loaded_journal);
                    break;
                }

                Err(e) => {
                    let policy = self.recovery_failure_policy();

                    error!(
                            "persistent actor (actor_id={actor_id}, persistence_key={persistence_key}) failed to recover - {error}, attempt={attempt}, failure_policy={failure_policy}",
                            actor_id = ctx.id(),
                            persistence_key = &persistence_key,
                            error = &e,
                            attempt = attempts,
                            failure_policy = &policy
                        );

                    self.on_recovery_err(e, ctx).await;

                    match policy {
                        RecoveryFailurePolicy::StopActor => {
                            ctx.stop(None);
                            return Recovery::Failed;
                        }

                        RecoveryFailurePolicy::Retry(retry_policy) => {
                            if !should_retry(ctx, &attempts, retry_policy).await {
                                return Recovery::Failed;
                            }
                        }

                        RecoveryFailurePolicy::Panic => panic!("Persistence failure"),
                    }
                }
            }

            attempts += 1;
        }

        let journal = journal.expect("no journal loaded");
        Recovery::Recovered(journal)
    }
}

pub struct RecoveredJournal<A: PersistentActor> {
    pub snapshot: Option<RecoveredPayload<A>>,
    pub messages: Option<Vec<RecoveredPayload<A>>>,
}

async fn load_journal<A: PersistentActor>(
    persistence_key: String,
    ctx: &mut ActorContext,
) -> Result<RecoveredJournal<A>, RecoveryErr> {
    let journal = ctx.persistence_mut().init_journal::<A>(persistence_key);
    trace!(persistence_key = journal.persistence_id(), "reading snapshot");

    let snapshot = journal
        .recover_snapshot()
        .await
        .map_err(RecoveryErr::Snapshot)?;

    trace!(persistence_key = journal.persistence_id(), "reading journal messages");

    let messages = journal
        .recover_messages()
        .await
        .map_err(RecoveryErr::Messages)?;

    Ok(RecoveredJournal { snapshot, messages })
}
