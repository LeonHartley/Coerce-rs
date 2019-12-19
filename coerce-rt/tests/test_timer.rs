use coerce_rt::actor::context::{ActorContext, ActorHandlerContext};
use coerce_rt::actor::scheduler::timer::{Timer, TimerTick};

use crate::util::*;
use coerce_rt::actor::message::{Handler, Message};
use std::time::Duration;

pub mod util;

#[macro_use]
extern crate serde;
extern crate serde_json;

extern crate chrono;

#[macro_use]
extern crate async_trait;

pub struct TestTimer {}

impl TimerTick for TestTimer {
    fn new() -> Self {
        TestTimer {}
    }
}

#[async_trait]
impl Handler<TestTimer> for TestActor {
    async fn handle(&mut self, message: TestTimer, ctx: &mut ActorHandlerContext) {
        self.counter += 1;
    }
}

#[tokio::test]
pub async fn test_timer() {
    create_trace_logger();

    let mut actor_ref = ActorContext::new()
        .new_tracked_actor(TestActor::new())
        .await
        .unwrap();

    let timer = Timer::start::<TestActor, TestTimer>(actor_ref.clone(), Duration::from_millis(101));

    tokio::time::delay_for(Duration::from_millis(400)).await;
    let counter_400ms_later = actor_ref.exec(|a| a.counter).await;

    tokio::time::delay_for(Duration::from_millis(400)).await;
    let counter_800ms_later = actor_ref.exec(|a| a.counter).await;

    let stop = timer.stop();

    let counter_now = actor_ref.exec(|a| a.counter).await.unwrap();
    tokio::time::delay_for(Duration::from_millis(200)).await;

    let counter_changed_after_stop = counter_now != actor_ref.exec(|a| a.counter).await.unwrap();

    assert_eq!(counter_400ms_later, Ok(4));
    assert_eq!(counter_800ms_later, Ok(8));
    assert_eq!(stop, true);
    assert_eq!(counter_changed_after_stop, false);
}
