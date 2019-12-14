use crate::util::{
    GetCounterRequest, GetStatusRequest, GetStatusResponse, SetStatusRequest, SetStatusResponse,
    TestActor, TestActorStatus,
};
use coerce_rt::actor::context::{ActorContext, ActorHandlerContext};
use coerce_rt::actor::message::{Exec, Handler, Message, MessageResult};
use coerce_rt::actor::{new_actor, Actor};

#[macro_use]
extern crate async_trait;

pub mod util;

#[async_trait]
impl Handler<GetStatusRequest> for TestActor {
    async fn handle(
        &mut self,
        _message: GetStatusRequest,
        _ctx: &mut ActorHandlerContext,
    ) -> GetStatusResponse {
        match self.status {
            Some(TestActorStatus::Active) => GetStatusResponse::Ok(TestActorStatus::Active),
            Some(TestActorStatus::Inactive) => GetStatusResponse::Ok(TestActorStatus::Inactive),
            _ => GetStatusResponse::None,
        }
    }
}

#[async_trait]
impl Handler<SetStatusRequest> for TestActor {
    async fn handle(
        &mut self,
        message: SetStatusRequest,
        _ctx: &mut ActorHandlerContext,
    ) -> SetStatusResponse {
        self.status = Some(message.0);

        SetStatusResponse::Ok
    }
}

#[async_trait]
impl Handler<GetCounterRequest> for TestActor {
    async fn handle(&mut self, _message: GetCounterRequest, _ctx: &mut ActorHandlerContext) -> i32 {
        self.counter
    }
}

#[async_trait]
impl Actor for TestActor {}

#[tokio::test]
pub async fn test_actor_req_res() {
    let mut actor_ref = new_actor(TestActor::new()).await.unwrap();

    let response = actor_ref.send(GetStatusRequest()).await;

    assert_eq!(response, Ok(GetStatusResponse::None));
}

#[tokio::test]
pub async fn test_actor_req_res_mutation() {
    let mut actor_ref = new_actor(TestActor::new()).await.unwrap();

    let initial_status = actor_ref.send(GetStatusRequest()).await;
    let _ = actor_ref
        .send(SetStatusRequest(TestActorStatus::Active))
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
    let mut actor_ref = new_actor(TestActor::new()).await.unwrap();
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
    let mut actor_ref = new_actor(TestActor::new()).await.unwrap();

    let a = actor_ref
        .exec(|mut actor| {
            actor.counter = 1;
        })
        .await;

    let _ = actor_ref.exec(|mut actor| actor.counter = 2).await;
    let _ = actor_ref.exec(|mut actor| actor.counter = 3).await;
    let _ = actor_ref.exec(|mut actor| actor.counter = 4).await;

    let counter = actor_ref.exec(|actor| actor.counter).await;
    assert_eq!(counter, Ok(4));
}

#[tokio::test]
pub async fn test_actor_notify() {
    let mut actor_ref = new_actor(TestActor::new()).await.unwrap();
//
//    for i in 1..=25 as i32 {
//        let _ = actor_ref
//            .notify_exec(move |mut actor| actor.counter = i)
//            .await;
//    }

//    let counter = actor_ref.exec(|actor| actor.counter).await;
//    assert_eq!(counter, Ok(25));
}
