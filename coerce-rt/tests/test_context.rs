use coerce_rt::actor::context::{ActorContext, ActorHandlerContext};
use coerce_rt::actor::message::{Exec, Handler, Message, MessageResult};
use coerce_rt::actor::scheduler::{ActorScheduler, RegisterActor};
use coerce_rt::actor::{get_actor, new_actor, Actor};

#[macro_use]
extern crate async_trait;

#[derive(Debug, Eq, PartialEq)]
pub enum TestActorStatus {
    Inactive,
    Active,
}

#[derive(Debug, Eq, PartialEq)]
pub struct GetStatusRequest();

#[derive(Debug, Eq, PartialEq)]
pub enum GetStatusResponse {
    Ok(TestActorStatus),
    None,
}

impl Message for GetStatusRequest {
    type Result = GetStatusResponse;
}

#[derive(Debug, Eq, PartialEq)]
pub struct SetStatusRequest(pub TestActorStatus);

#[derive(Debug, Eq, PartialEq)]
pub enum SetStatusResponse {
    Ok,
    Unsuccessful,
}

impl Message for SetStatusRequest {
    type Result = SetStatusResponse;
}

#[derive(Debug, Eq, PartialEq)]
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
pub async fn test_context_get_actor() {
    let mut ctx = ActorContext::new();

    let mut actor_ref = ctx.new_actor(TestActor::new()).await.unwrap();

    let _ = actor_ref
        .exec(|mut actor| {
            actor.counter = 1337;
        })
        .await;

    let mut actor = ctx.get_actor::<TestActor>(actor_ref.id).await.unwrap();

    let counter = actor.exec(|actor| actor.counter).await;

    assert_eq!(counter, Ok(1337));
}
