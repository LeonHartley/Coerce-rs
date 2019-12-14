use coerce_rt::actor::context::{ActorContext, ActorStatus};
use coerce_rt::actor::lifecycle::Status;
use coerce_rt::actor::scheduler::ActorRefError;
use coerce_rt::actor::Actor;

use util::TestActor;

#[macro_use]
extern crate async_trait;

pub mod util;

#[async_trait]
impl Actor for TestActor {}

#[tokio::test]
pub async fn test_actor_lifecycle_started() {
    let ctx = ActorContext::new();
    let mut actor_ref = ctx.lock().unwrap().new_actor(TestActor::new());

    let status = actor_ref.status().await;

    actor_ref.stop().await;
    assert_eq!(status, Ok(ActorStatus::Started))
}

#[tokio::test]
pub async fn test_actor_lifecycle_stopping() {
    let ctx = ActorContext::new();
    let mut actor_ref = ctx.lock().unwrap().new_actor(TestActor::new());

    let status = actor_ref.status().await;
    let stopping = actor_ref.stop().await;
    let msg_send = actor_ref.status().await;

    assert_eq!(status, Ok(ActorStatus::Started));
    assert_eq!(stopping, Ok(ActorStatus::Stopping));
    assert_eq!(msg_send, Err(ActorRefError::ActorUnavailable));
}
