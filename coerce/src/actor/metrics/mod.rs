use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub struct ActorSystemMetrics {
    total_messages_processed: AtomicU64,
    total_actors_started: AtomicU64,
    total_actors_stopped: AtomicU64,
}

impl ActorSystemMetrics {
    pub fn new() -> ActorSystemMetrics {
        ActorSystemMetrics {
            total_messages_processed: AtomicU64::new(0),
            total_actors_started: AtomicU64::new(0),
            total_actors_stopped: AtomicU64::new(0),
        }
    }

    #[inline]
    pub fn get_msgs_processed(&self) -> u64 {
        get(&self.total_messages_processed)
    }

    #[inline]
    pub fn get_actors_started(&self) -> u64 {
        get(&self.total_actors_started)
    }

    #[inline]
    pub fn get_actors_stopped(&self) -> u64 {
        get(&self.total_actors_stopped)
    }

    #[inline]
    pub fn increment_msgs_processed(&self) {
        incr(&self.total_messages_processed);
    }

    #[inline]
    pub fn increment_actors_started(&self) {
        incr(&self.total_actors_started);
    }

    #[inline]
    pub fn increment_actors_stopped(&self) {
        incr(&self.total_actors_stopped);
    }
}

#[inline]
fn get(number: &AtomicU64) -> u64 {
    number.load(Relaxed)
}

#[inline]
fn incr(number: &AtomicU64) {
    let _previous = number.fetch_add(1, Ordering::Relaxed);
}
