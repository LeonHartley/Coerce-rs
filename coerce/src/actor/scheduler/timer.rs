use crate::actor::message::{Handler, Message};
use crate::actor::{Actor, LocalActorRef};
use std::ops::Add;
use tracing::trace;

use std::time::Duration;
use tokio::sync::oneshot;
use tokio::time;
use tokio::time::Instant;
use uuid::Uuid;

pub trait TimerTick: Message + Clone {}

enum TimerMode {
    Notify,
    Send,
}

pub struct Timer {
    stop: oneshot::Sender<bool>,
}

impl Timer {
    pub fn start_immediately<A: Actor, T: TimerTick>(
        actor: LocalActorRef<A>,
        tick: Duration,
        msg: T,
    ) -> Timer
    where
        A: 'static + Handler<T> + Sync + Send,
        T::Result: 'static + Sync + Send,
    {
        let (stop, stop_rx) = oneshot::channel();
        tokio::spawn(timer_loop(tick, msg, actor, stop_rx, true, TimerMode::Send));

        Timer { stop }
    }

    pub fn start<A: Actor, T: TimerTick>(actor: LocalActorRef<A>, tick: Duration, msg: T) -> Timer
    where
        A: 'static + Handler<T> + Sync + Send,
        T::Result: 'static + Sync + Send,
    {
        let (stop, stop_rx) = oneshot::channel();
        tokio::spawn(timer_loop(
            tick,
            msg,
            actor,
            stop_rx,
            false,
            TimerMode::Send,
        ));

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

async fn timer_loop<A: Actor, T: TimerTick>(
    tick: Duration,
    msg: T,
    actor: LocalActorRef<A>,
    mut stop_rx: oneshot::Receiver<bool>,
    tick_immediately: bool,
    mode: TimerMode,
) where
    A: Handler<T>,
{
    let start = if tick_immediately {
        Instant::now()
    } else {
        Instant::now().add(tick)
    };

    let mut interval = time::interval_at(start, tick);
    let timer_id = Uuid::new_v4();

    interval.tick().await;

    trace!("{} - timer starting", &timer_id);

    loop {
        if stop_rx.try_recv().is_ok() {
            break;
        }

        trace!("{} - timer tick", &timer_id);

        let now = Instant::now();

        match mode {
            TimerMode::Notify => {
                if actor.notify(msg.clone()).is_err() {
                    break;
                }
            }
            TimerMode::Send => {
                if actor.send(msg.clone()).await.is_err() {
                    break;
                }

                interval.reset();
            }
        }

        trace!(
            "{} - tick res received in {}ms",
            &timer_id,
            now.elapsed().as_millis()
        );
        interval.tick().await;
    }

    trace!("{} - timer finished", timer_id);
}
