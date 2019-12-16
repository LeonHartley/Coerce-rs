use crate::util::create_trace_logger;
use chrono::Local;
use coerce_rt::actor::context::{ActorContext, ActorHandlerContext};
use coerce_rt::actor::message::{Handler, Message};
use coerce_rt::actor::{new_actor, Actor, ActorRef};
use coerce_rt::remote::context::RemoteActorContext;
use coerce_rt::remote::handler::RemoteActorMessageMarker;
use coerce_rt::remote::*;
use std::mem::forget;

pub mod util;

#[macro_use]
extern crate serde;
extern crate serde_json;

extern crate chrono;

#[macro_use]
extern crate async_trait;

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
pub enum TestActorStatus {
    Inactive,
    Active,
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct GetStatusRequest();

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
pub enum GetStatusResponse {
    Ok(TestActorStatus),
    None,
}

impl Message for GetStatusRequest {
    type Result = GetStatusResponse;
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct SetStatusRequest {
    status: TestActorStatus,
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
pub enum SetStatusResponse {
    Ok,
    Unsuccessful,
}

impl Message for SetStatusRequest {
    type Result = SetStatusResponse;
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct GetCounterRequest();

impl Message for GetCounterRequest {
    type Result = i32;
}

pub struct TestActor {
    pub status: Option<TestActorStatus>,
    pub counter: i32,
}

impl TestActor {
    pub fn new() -> TestActor {
        TestActor {
            status: None,
            counter: 0,
        }
    }
}

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
        self.status = Some(message.status);

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

pub struct EchoActor {}

#[async_trait]
impl Actor for EchoActor {}

#[async_trait]
impl Handler<GetCounterRequest> for EchoActor {
    async fn handle(&mut self, _message: GetCounterRequest, _ctx: &mut ActorHandlerContext) -> i32 {
        42
    }
}

#[tokio::test]
pub async fn test_remote_handler_types() {
    let echo_get_counter = "EchoActor.GetCounterRequest".to_string();
    let test_get_status = "TestActor.GetStatusRequest".to_string();
    let test_set_status = "TestActor.SetStatusRequest".to_string();

    let mut remote = RemoteActorContext::builder()
        .with_actor_context(ActorContext::new())
        .with_handler::<TestActor, SetStatusRequest>(test_set_status.clone().as_ref())
        .with_handler::<TestActor, GetStatusRequest>(test_get_status.clone().as_ref())
        .with_handler::<EchoActor, GetCounterRequest>(echo_get_counter.clone().as_ref())
        .build()
        .await;

    assert_eq!(
        remote.handler_name::<EchoActor, GetCounterRequest>().await,
        Some(echo_get_counter)
    );
    assert_eq!(
        remote.handler_name::<TestActor, SetStatusRequest>().await,
        Some(test_set_status)
    );
    assert_eq!(
        remote.handler_name::<TestActor, GetStatusRequest>().await,
        Some(test_get_status)
    );

    forget(remote);
}

#[tokio::test]
pub async fn test_remote_handle_from_json() {
    create_trace_logger();

    let mut ctx = ActorContext::new();
    let mut actor = ctx.new_actor(TestActor::new()).await.unwrap();

    let mut remote = RemoteActorContext::builder()
        .with_actor_context(ctx)
        .with_handler::<TestActor, SetStatusRequest>("TestActor.SetStatusRequest")
        .with_handler::<TestActor, GetStatusRequest>("TestActor.GetStatusRequest")
        .with_handler::<EchoActor, GetCounterRequest>("EchoActor.GetCounterRequest")
        .build()
        .await;

    let initial_status = actor.send(GetStatusRequest()).await;

    let res = remote
        .handle(
            "TestActor.SetStatusRequest".to_string(),
            actor.id,
            b"{\"status\": \"Active\"}",
        )
        .await;

    let current_status = actor.send(GetStatusRequest()).await;

    let handler_name = remote.handler_name::<EchoActor, GetCounterRequest>().await;

    assert_eq!(
        handler_name,
        Some("EchoActor.GetCounterRequest".to_string())
    );

    assert_eq!(res, Ok(b"\"Ok\"".to_vec()));

    assert_eq!(initial_status, Ok(GetStatusResponse::None));
    assert_eq!(
        current_status,
        Ok(GetStatusResponse::Ok(TestActorStatus::Active))
    );

    forget(remote);
}
