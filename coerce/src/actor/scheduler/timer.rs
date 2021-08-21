use crate::actor::message::{Handler, Message};
use crate::actor::{Actor, LocalActorRef};

use std::time::{Duration, Instant};
use uuid::Uuid;

pub trait TimerTick: Message {}

pub struct Timer {
    stop: tokio::sync::oneshot::Sender<bool>,
}

impl Timer {
    pub fn start<A: Actor, T: TimerTick>(actor: LocalActorRef<A>, tick: Duration, msg: T) -> Timer
    where
        A: 'static + Handler<T> + Sync + Send,
        T: 'static + Clone + Sync + Send,
        T::Result: 'static + Sync + Send,
    {
        let (stop, stop_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(timer_loop(tick, msg, actor, stop_rx));

        Timer { stop }
    }

    pub fn stop(self) -> bool {
        if self.stop.send(true).is_ok() {
            true
        } else {
            false
        }
    }
}

pub async fn timer_loop<A: Actor, T: TimerTick>(
    tick: Duration,
    msg: T,
    actor: LocalActorRef<A>,
    mut stop_rx: tokio::sync::oneshot::Receiver<bool>,
) where
    A: Handler<T>,
    T: 'static + Clone + Sync + Send,
{
    let mut interval = tokio::time::interval_at(tokio::time::Instant::now(), tick);
    interval.tick().await;

    loop {
        if stop_rx.try_recv().is_ok() {
            break;
        }

        if actor.send(msg.clone()).await.is_err() {
            break;
        }

        interval.tick().await;
    }
}
