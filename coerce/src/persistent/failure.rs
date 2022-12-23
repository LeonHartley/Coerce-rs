use crate::actor::context::ActorContext;

use std::fmt;
use std::fmt::{Display, Formatter};
use std::time::Duration;

#[derive(Copy, Clone)]
pub enum RecoveryFailurePolicy {
    Retry(Retry),
    StopActor,
    Panic,
}

#[derive(Copy, Clone)]
pub enum PersistFailurePolicy {
    Retry(Retry),
    ReturnErr,
    StopActor,
    Panic,
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

pub(crate) async fn should_retry(ctx: &mut ActorContext, attempts: &usize, retry: Retry) -> bool {
    match retry {
        Retry::UntilSuccess { delay } => {
            if let Some(delay) = delay {
                tokio::time::sleep(delay).await;
            }
        }

        Retry::MaxAttempts {
            max_attempts,
            delay,
        } => {
            if attempts >= &max_attempts {
                ctx.stop(None);
                return false;
            }

            if let Some(delay) = delay {
                tokio::time::sleep(delay).await;
            }
        }
    }
    return true;
}

impl Display for RecoveryFailurePolicy {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match &self {
            RecoveryFailurePolicy::StopActor => write!(f, "StopActor"),
            RecoveryFailurePolicy::Retry(r) => write!(f, "Retry({})", r),
            RecoveryFailurePolicy::Panic => write!(f, "Panic"),
        }
    }
}

impl Display for PersistFailurePolicy {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match &self {
            PersistFailurePolicy::StopActor => write!(f, "StopActor"),
            PersistFailurePolicy::Retry(r) => write!(f, "Retry({})", r),
            PersistFailurePolicy::Panic => write!(f, "Panic"),
            PersistFailurePolicy::ReturnErr => write!(f, "ReturnErr"),
        }
    }
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

impl Default for RecoveryFailurePolicy {
    fn default() -> Self {
        RecoveryFailurePolicy::Retry(Retry::UntilSuccess {
            delay: Some(Duration::from_millis(500)),
        })
    }
}

impl Default for PersistFailurePolicy {
    fn default() -> Self {
        PersistFailurePolicy::Retry(Retry::UntilSuccess {
            delay: Some(Duration::from_millis(500)),
        })
    }
}
