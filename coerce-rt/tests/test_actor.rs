use coerce_rt::actor::message::{Message, Handler, MessageResult};
use coerce_rt::actor::Actor;
use coerce_rt::actor::context::ActorContext;

#[macro_use]
extern crate async_trait;

pub struct TestActor {
    status: Option<TestActorStatus>
}

#[derive(Debug, Eq, PartialEq)]
pub enum TestActorStatus {
    Inactive,
    Active
}

#[derive(Debug, Eq, PartialEq)]
pub struct GetStatusRequest {}

#[derive(Debug, Eq, PartialEq)]
pub enum GetStatusResponse {
    Ok(TestActorStatus),
    None
}

impl Message for GetStatusRequest {
    type Result = GetStatusResponse;
}

#[derive(Debug, Eq, PartialEq)]
pub struct SetStatusRequest(TestActorStatus);

#[derive(Debug, Eq, PartialEq)]
pub enum SetStatusResponse {
    Ok,
    Unsuccessful
}

impl Message for SetStatusRequest {
    type Result = SetStatusResponse;
}

#[async_trait]
impl Handler<GetStatusRequest> for TestActor {
    async fn handle(&mut self, message: GetStatusRequest) -> GetStatusResponse {
        match self.status {
            Some(TestActorStatus::Active) => GetStatusResponse::Ok(TestActorStatus::Active),
            Some(TestActorStatus::Inactive) => GetStatusResponse::Ok(TestActorStatus::Inactive),
            _ => GetStatusResponse::None
        }
    }
}

#[async_trait]
impl Handler<SetStatusRequest> for TestActor {
    async fn handle(&mut self, message: SetStatusRequest) -> SetStatusResponse {
        self.status = Some(message.0);

        SetStatusResponse::Ok
    }
}


impl TestActor {
    pub fn new() -> TestActor {
        TestActor { status: None }
    }
}

#[async_trait]
impl Actor for TestActor {}

#[tokio::test]
pub async fn test_actor_req_res() {
    let ctx = ActorContext::new();
    let mut actor_ref = ctx.lock().unwrap().new_actor(TestActor::new());

    let response = actor_ref.send(GetStatusRequest {}).await;

    assert_eq!(response, Ok(GetStatusResponse::None));
}

#[tokio::test]
pub async fn test_actor_req_res_mutation() {
    let ctx = ActorContext::new();
    let mut actor_ref = ctx.lock().unwrap().new_actor(TestActor::new());

    let initial_status = actor_ref.send(GetStatusRequest {}).await;
    let _ = actor_ref.send(SetStatusRequest(TestActorStatus::Active)).await;
    let current_status = actor_ref.send(GetStatusRequest {}).await;

    assert_eq!(initial_status, Ok(GetStatusResponse::None));
    assert_eq!(current_status, Ok(GetStatusResponse::Ok(TestActorStatus::Active)));
}