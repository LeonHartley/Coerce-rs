use coerce_rt::actor::context::ActorContext;
use util::*;

pub mod util;

#[macro_use]
extern crate serde;
extern crate serde_json;

extern crate chrono;

#[macro_use]
extern crate async_trait;

#[tokio::test]
pub async fn test_actor_req_res() {
    let mut actor_ref = ActorContext::new()
        .new_actor(TestActor::new())
        .await
        .unwrap();

    let response = actor_ref.send(GetStatusRequest()).await;

    assert_eq!(response, Ok(GetStatusResponse::None));
}

#[tokio::test]
pub async fn test_actor_req_res_mutation() {
    let mut actor_ref = ActorContext::new()
        .new_actor(TestActor::new())
        .await
        .unwrap();

    let initial_status = actor_ref.send(GetStatusRequest()).await;
    let _ = actor_ref
        .send(SetStatusRequest {
            status: TestActorStatus::Active,
        })
        .await;

    let current_status = actor_ref.send(GetStatusRequest()).await;

    assert_eq!(initial_status, Ok(GetStatusResponse::None));
    assert_eq!(
        current_status,
        Ok(GetStatusResponse::Ok(TestActorStatus::Active))
    );
}

#[tokio::test]
pub async fn test_actor_exec_mutation() {
    let mut actor_ref = ActorContext::new()
        .new_actor(TestActor::new())
        .await
        .unwrap();
    let initial_status = actor_ref.send(GetStatusRequest()).await;

    actor_ref
        .exec(|mut actor| {
            actor.status = Some(TestActorStatus::Active);
        })
        .await;

    let current_status = actor_ref.send(GetStatusRequest()).await;
    assert_eq!(initial_status, Ok(GetStatusResponse::None));
    assert_eq!(
        current_status,
        Ok(GetStatusResponse::Ok(TestActorStatus::Active))
    );
}

#[tokio::test]
pub async fn test_actor_exec_chain_mutation() {
    let mut actor_ref = ActorContext::new()
        .new_actor(TestActor::new())
        .await
        .unwrap();

    let _a = actor_ref
        .exec(|mut actor| {
            actor.counter = 1;
        })
        .await;

    let _ = actor_ref.exec(|mut actor| actor.counter = 2).await;

    let counter = actor_ref.exec(|actor| actor.counter).await;
    assert_eq!(counter, Ok(2));
}

#[tokio::test]
pub async fn test_actor_notify() {
    let mut actor_ref = ActorContext::new()
        .new_actor(TestActor::new())
        .await
        .unwrap();

    for i in 1..=25 as i32 {
        let _ = actor_ref
            .notify_exec(move |mut actor| actor.counter = i)
            .await;
    }

    let counter = actor_ref.exec(|actor| actor.counter).await;
    assert_eq!(counter, Ok(25));
}
