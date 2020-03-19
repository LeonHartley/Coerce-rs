use coerce_rt::actor::context::ActorContext;
use coerce_rt::actor::{get_actor, new_actor, ActorId};
use util::*;
use uuid::Uuid;

pub mod util;

#[macro_use]
extern crate serde;
extern crate serde_json;

extern crate chrono;

#[macro_use]
extern crate async_trait;

#[tokio::test]
pub async fn test_context_global_get_actor() {
    let mut actor_ref = new_actor(TestActor::new()).await.unwrap();

    let _ = actor_ref
        .exec(|mut actor| {
            actor.counter = 1337;
        })
        .await;

    let mut actor = get_actor::<TestActor>(actor_ref.id).await.unwrap();

    let counter = actor.exec(|actor| actor.counter).await;

    assert_eq!(counter, Ok(1337));
}

#[tokio::test]
pub async fn test_context_get_tracked_actor() {
    let mut ctx = ActorContext::new();

    let mut actor_ref = ctx.new_tracked_actor(TestActor::new()).await.unwrap();

    let _ = actor_ref
        .exec(|mut actor| {
            actor.counter = 1337;
        })
        .await;

    let mut actor = ctx
        .get_tracked_actor::<TestActor>(actor_ref.id)
        .await
        .unwrap();
    let counter = actor.exec(|actor| actor.counter).await;

    assert_eq!(counter, Ok(1337));
}

#[tokio::test]
pub async fn test_context_get_actor_not_found() {
    let mut ctx = ActorContext::new();
    let actor = ctx
        .get_tracked_actor::<TestActor>(format!("{}", Uuid::new_v4()))
        .await;

    assert_eq!(actor.is_none(), true);
}

#[tokio::test]
pub async fn test_context_stop_tracked_actor_get_not_found() {
    let mut ctx = ActorContext::new();

    let mut actor_ref = ctx.new_tracked_actor(TestActor::new()).await.unwrap();

    let _ = actor_ref
        .exec(|mut actor| {
            actor.counter = 1337;
        })
        .await;

    let _stop = actor_ref.stop().await;

    let actor = ctx.get_tracked_actor::<TestActor>(actor_ref.id).await;

    assert_eq!(actor.is_none(), true);
}
