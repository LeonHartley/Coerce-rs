use coerce::actor::system::ActorSystem;
use coerce::actor::{get_actor, new_actor, new_actor_id};
use coerce::TraceCollector;
use tracing_subscriber::prelude::*;
use util::*;

pub mod util;

#[macro_use]
extern crate serde;

#[macro_use]
extern crate async_trait;

#[tokio::test]
pub async fn test_context_global_get_actor() {
    let actor_ref = new_actor(TestActor::new()).await.unwrap();

    let _ = actor_ref
        .exec(|mut actor| {
            actor.counter = 1337;
        })
        .await;

    let actor = get_actor::<TestActor>(actor_ref.id).await.unwrap();

    let counter = actor.exec(|actor| actor.counter).await;

    assert_eq!(counter, Ok(1337));
}

#[tokio::test]
pub async fn test_context_get_tracked_actor() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .init();

    let ctx = ActorSystem::new();

    let actor_ref = ctx.new_tracked_actor(TestActor::new()).await.unwrap();

    let _ = actor_ref
        .exec(|mut actor| {
            actor.counter = 1337;
        })
        .await;

    let actor = ctx
        .get_tracked_actor::<TestActor>(actor_ref.id)
        .await
        .unwrap();
    let counter = actor.exec(|actor| actor.counter).await;

    assert_eq!(counter, Ok(1337));
}

#[tokio::test]
pub async fn test_context_get_actor_not_found() {
    let ctx = ActorSystem::new();
    let actor = ctx.get_tracked_actor::<TestActor>(new_actor_id()).await;

    assert_eq!(actor.is_none(), true);
}

#[tokio::test]
pub async fn test_context_stop_tracked_actor_get_not_found() {
    util::create_trace_logger();

    let ctx = ActorSystem::new();

    let actor_ref = ctx.new_tracked_actor(TestActor::new()).await.unwrap();

    let _ = actor_ref
        .exec(|mut actor| {
            actor.counter = 1337;
        })
        .await;

    let _stop = actor_ref.stop().await;

    let actor = ctx.get_tracked_actor::<TestActor>(actor_ref.id).await;

    assert_eq!(actor.is_none(), true);
}
