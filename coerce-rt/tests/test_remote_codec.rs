use coerce_rt::actor::context::{ActorContext, ActorHandlerContext};
use coerce_rt::actor::message::{Handler, Message};
use coerce_rt::actor::Actor;
use coerce_rt::remote::context::RemoteActorContext;
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
pub async fn test_remote_create_message() {
    util::create_trace_logger();

    let test_set_status = "TestActor.SetStatusRequest".to_string();

    let mut remote = RemoteActorContext::builder()
        .with_actor_context(ActorContext::new())
        .with_handler::<TestActor, SetStatusRequest>(test_set_status.clone().as_ref())
        .build()
        .await;

    let actor = remote.inner().new_actor(TestActor::new()).await.unwrap();
    let message = remote
        .create_message::<TestActor, SetStatusRequest>(
            &actor,
            SetStatusRequest {
                status: TestActorStatus::Active,
            },
        )
        .await
        .unwrap();

    assert_eq!(message.actor_id, actor.id);
    assert_eq!(message.handler_type, test_set_status);
    assert_eq!(message.message, "{\"status\":\"Active\"}");

    forget(remote);
}
