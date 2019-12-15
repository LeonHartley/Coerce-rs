use coerce_rt::actor::context::{ActorContext, ActorHandlerContext};
use coerce_rt::actor::message::{Handler, Message};
use coerce_rt::actor::remote::*;
use coerce_rt::actor::{new_actor, Actor, ActorRef};

#[macro_use]
extern crate serde;
extern crate serde_json;

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

#[tokio::test]
pub async fn test_remote_handle_from_json() {
    let mut actor = new_actor(TestActor::new()).await.unwrap();

    let mut remote = RemoteActorContext::builder()
        .with_handler::<TestActor, SetStatusRequest>("TestActor.SetStatusRequest")
        .build();

    let initial_status = actor.send(GetStatusRequest()).await;

    let res = remote
        .handle(
            "TestActor.SetStatusRequest".to_string(),
            actor.id,
            b"{\"status\": \"Active\"}",
        )
        .await;

    let current_status = actor.send(GetStatusRequest()).await;

    assert_eq!(res, Ok(b"\"Ok\"".to_vec()));

    assert_eq!(initial_status, Ok(GetStatusResponse::None));
    assert_eq!(
        current_status,
        Ok(GetStatusResponse::Ok(TestActorStatus::Active))
    );
}
