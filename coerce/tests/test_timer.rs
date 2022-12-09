use coerce::actor::context::ActorContext;
use coerce::actor::message::{Handler, Message};
use coerce::actor::scheduler::timer::{Timer, TimerTick};
use coerce::actor::system::ActorSystem;
use coerce::actor::Actor;
use std::time::{Duration, Instant};

pub mod util;

#[macro_use]
extern crate serde;

#[macro_use]
extern crate async_trait;

#[derive(Clone)]
pub struct TestTimer {}

struct TimerActor {
    ticks: Vec<Instant>,
}

impl Message for TestTimer {
    type Result = ();
}

impl TimerTick for TestTimer {}

impl Actor for TimerActor {}

#[async_trait]
impl Handler<TestTimer> for TimerActor {
    async fn handle(&mut self, _message: TestTimer, _ctx: &mut ActorContext) {
        self.ticks.push(Instant::now());
    }
}

#[ignore]
#[tokio::test]
pub async fn test_timer() {
    util::create_trace_logger();
    let actor_ref = ActorSystem::new()
        .new_tracked_actor(TimerActor { ticks: vec![] })
        .await
        .unwrap();

    const TICK_DURATION: u64 = 1;
    let start = Instant::now();
    let timer = Timer::start(
        actor_ref.clone(),
        Duration::from_secs(TICK_DURATION),
        TestTimer {},
    );

    tokio::time::sleep(Duration::from_secs(5)).await;

    timer.stop();
    let ticks_after_stopping = actor_ref.exec(|a| a.ticks.clone()).await.unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;
    let ticks_after_stopping_and_waiting = actor_ref.exec(|a| a.ticks.len()).await.unwrap();

    let mut previous_tick = None;
    for tick in &ticks_after_stopping {
        if let Some(previous_tick) = previous_tick {
            tracing::info!("{}", tick.duration_since(previous_tick).as_secs());
            assert!(tick.duration_since(previous_tick).as_secs() >= TICK_DURATION);
        } else {
            previous_tick = Some(*tick);
            tracing::info!("{}", tick.duration_since(start).as_secs());
            assert!(tick.duration_since(start).as_secs() >= TICK_DURATION);
        }
    }
    assert_eq!(ticks_after_stopping.len(), ticks_after_stopping_and_waiting);
}
