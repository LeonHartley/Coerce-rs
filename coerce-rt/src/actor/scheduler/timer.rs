use crate::actor::message::{Handler, Message};
use crate::actor::{Actor, ActorRef};
use log::trace;
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;

pub trait TimerTick: Message {
    fn new() -> Self
    where
        Self: 'static + Sync + Send;
}

impl<T> Message for T
where
    T: TimerTick,
{
    type Result = ();
}

pub struct Timer {
    stop: tokio::sync::oneshot::Sender<bool>,
}

impl Timer {
    pub fn start<A: Actor, T: TimerTick>(actor: ActorRef<A>, tick: Duration) -> Timer
    where
        A: 'static + Handler<T> + Sync + Send,
        T: 'static + Sync + Send,
        T::Result: 'static + Sync + Send,
    {
        let (stop, mut stop_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(timer_loop(tick, actor, stop_rx));

        Timer { stop }
    }

    pub fn stop(self) -> bool {
        if let Ok(()) = self.stop.send(true) {
            true
        } else {
            false
        }
    }
}

pub async fn timer_loop<A: Actor, T: TimerTick>(
    tick: Duration,
    mut actor: ActorRef<A>,
    mut stop_rx: tokio::sync::oneshot::Receiver<bool>,
) where
    A: 'static + Handler<T> + Sync + Send,
    T: 'static + Sync + Send,
    T::Result: 'static + Sync + Send,
{
    let mut interval = tokio::time::interval_at(tokio::time::Instant::now(), tick);
    let timer_id = Uuid::new_v4();

    interval.tick().await;

    trace!(target: "Timer", "{} - timer starting", &timer_id);

    loop {
        if stop_rx.try_recv().is_ok() {
            break;
        }

        trace!(target: "Timer", "{} - timer tick", &timer_id);

        let now = Instant::now();
        actor.send(T::new()).await;
        trace!(target: "Timer", "{} - tick res received in {}ms", &timer_id, now.elapsed().as_millis());
        interval.tick().await;
    }

    trace!(target: "Timer", "{} - timer finished", timer_id);
}
