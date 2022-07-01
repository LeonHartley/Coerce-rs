use coerce::actor::context::ActorStatus;
use coerce::actor::system::ActorSystem;
use coerce::actor::{ActorRefErr, IntoActor};
use futures::future::join_all;
use std::time::Duration;
use tokio::time::sleep;
use util::*;

pub mod util;

#[macro_use]
extern crate serde;

#[macro_use]
extern crate async_trait;

#[tokio::test]
pub async fn test_actor_lifecycle_started() {
    let actor_ref = ActorSystem::new()
        .new_anon_actor(TestActor::new())
        .await
        .unwrap();

    let status = actor_ref.status().await;

    actor_ref.stop().await.expect("actor stop");
    assert_eq!(status, Ok(ActorStatus::Started))
}

#[ignore]
#[tokio::test(flavor = "multi_thread", worker_threads = 20)]
pub async fn test_actor_start_100k_and_stop() {
    let system = ActorSystem::new();

    // let mut tasks = vec![];
    for i in 0..10_000_000 {
        // let sys = system.clone();
        // tasks.push(async move {
        let actor_name = format!("actor-{}", i);
        let _actor = TestActor::new()
            .into_actor(Some(actor_name), &system)
            .await
            .expect("start actor");
        // })
    }

    println!("done 10m actors");
    // let _ = join_all(tasks).await;

    println!("sleeping 30s");
    let _ = sleep(Duration::from_secs(30)).await;

    println!("shutting down");
    system.shutdown().await;
    println!("shutdown complete");

    println!("sleeping 30s");
    let _ = sleep(Duration::from_secs(30)).await;
}

#[tokio::test]
pub async fn test_actor_lifecycle_stopping() {
    let actor_ref = ActorSystem::new()
        .new_anon_actor(TestActor::new())
        .await
        .unwrap();

    let status = actor_ref.status().await;
    let stopping = actor_ref.stop().await;
    let msg_send = actor_ref.status().await;

    assert_eq!(status, Ok(ActorStatus::Started));
    assert_eq!(stopping, Ok(()));
    assert_eq!(msg_send, Err(ActorRefErr::InvalidRef));
}
