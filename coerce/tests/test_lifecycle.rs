use coerce::actor::context::ActorStatus;
use coerce::actor::system::ActorSystem;
use coerce::actor::ActorRefErr;
use util::*;

pub mod util;

#[macro_use]
extern crate serde;
extern crate serde_json;

extern crate chrono;

#[macro_use]
extern crate async_trait;

#[tokio::test]
pub async fn test_actor_lifecycle_started() {
    let mut actor_ref = ActorSystem::new()
        .new_anon_actor(TestActor::new())
        .await
        .unwrap();

    let status = actor_ref.status().await;

    actor_ref.stop().await.expect("actor stop");
    assert_eq!(status, Ok(ActorStatus::Started))
}

#[tokio::test]
pub async fn test_actor_lifecycle_stopping() {
    let mut actor_ref = ActorSystem::new()
        .new_anon_actor(TestActor::new())
        .await
        .unwrap();

    let status = actor_ref.status().await;
    let stopping = actor_ref.stop().await;
    let msg_send = actor_ref.status().await;

    assert_eq!(status, Ok(ActorStatus::Started));
    assert_eq!(stopping, Ok(ActorStatus::Stopping));
    assert_eq!(msg_send, Err(ActorRefErr::ActorUnavailable));
}
