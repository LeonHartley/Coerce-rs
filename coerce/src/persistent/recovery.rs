use crate::actor::context::ActorContext;
use crate::persistent::PersistentActor;

pub struct ActorRecovery;

impl ActorRecovery {

}

async fn load_journal<A: PersistentActor>(
    persistence_key: String,
    ctx: &mut ActorContext,
) -> Result<RecoveredJournal<A>, JournalRecoveryErr> {
    let journal = ctx.persistence_mut().init_journal::<A>(persistence_key);

    let snapshot = journal
        .recover_snapshot()
        .await
        .map_err(JournalRecoveryErr::Snapshot)?;

    let messages = journal
        .recover_messages()
        .await
        .map_err(JournalRecoveryErr::Messages)?;

    Ok(RecoveredJournal { snapshot, messages })
}


#[derive(Copy, Clone)]
pub enum Retry {
    UntilSuccess {
        delay: Option<Duration>,
    },
    MaxAttempts {
        max_attempts: usize,
        delay: Option<Duration>,
    },
}

#[derive(Copy, Clone)]
pub enum RecoveryFailurePolicy {
    StopActor,
    RetryRecovery(Retry),
    Panic,
}

impl Display for Retry {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match &self {
            Retry::UntilSuccess { delay } => {
                if let Some(delay) = delay.as_ref() {
                    write!(f, "UntilSuccess(delay={} millis)", delay.as_millis())
                } else {
                    write!(f, "UntilSuccess(delay=None)")
                }
            }
            Retry::MaxAttempts {
                max_attempts,
                delay,
            } => {
                if let Some(delay) = delay.as_ref() {
                    write!(
                        f,
                        "MaxAttempts(max_attempts={}, delay={} millis)",
                        max_attempts,
                        delay.as_millis()
                    )
                } else {
                    write!(f, "MaxAttempts(max_attempts={}, delay=None)", max_attempts)
                }
            }
        }
    }
}

impl Display for RecoveryFailurePolicy {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match &self {
            RecoveryFailurePolicy::StopActor => write!(f, "StopActor"),
            RecoveryFailurePolicy::RetryRecovery(r) => write!(f, "Retry({})", r),
            RecoveryFailurePolicy::Panic => write!(f, "Panic"),
        }
    }
}

impl Default for RecoveryFailurePolicy {
    fn default() -> Self {
        RecoveryFailurePolicy::RetryRecovery(Retry::UntilSuccess {
            delay: Some(Duration::from_millis(500)),
        })
    }
}

struct RecoveredJournal<A: PersistentActor> {
    snapshot: Option<RecoveredPayload<A>>,
    messages: Option<Vec<RecoveredPayload<A>>>,
}

#[derive(Debug)]
pub enum JournalRecoveryErr {
    Snapshot(anyhow::Error),
    Messages(anyhow::Error),
}

impl Display for JournalRecoveryErr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match &self {
            JournalRecoveryErr::Snapshot(e) => {
                write!(f, "Snapshot recovery error: {}", e)
            }
            JournalRecoveryErr::Messages(e) => {
                write!(f, "Message recovery error: {}", e)
            }
        }
    }
}

impl Error for JournalRecoveryErr {}