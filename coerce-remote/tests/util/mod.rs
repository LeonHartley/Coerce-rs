use chrono::Local;
use coerce_rt::actor::context::ActorHandlerContext;
use coerce_rt::actor::message::{Handler, Message};
use coerce_rt::actor::Actor;

use env_logger::Builder;
use log::LevelFilter;
use std::io::Write;

pub mod test;

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
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

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct SetStatusRequest {
    pub status: TestActorStatus,
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
        42
    }
}

#[async_trait]
impl Actor for TestActor {}

pub struct EchoActor {}

impl EchoActor {
    pub fn new() -> EchoActor {
        EchoActor {}
    }
}

#[async_trait]
impl Actor for EchoActor {}

#[async_trait]
impl Handler<GetCounterRequest> for EchoActor {
    async fn handle(&mut self, _message: GetCounterRequest, _ctx: &mut ActorHandlerContext) -> i32 {
        42
    }
}

pub fn create_trace_logger() {
    Builder::new()
        .format(|buf, record| {
            writeln!(
                buf,
                "{} [{}] {} - {}",
                Local::now().format("%Y-%m-%dT%H:%M:%S"),
                record.level(),
                record.target(),
                record.args(),
            )
        })
        .filter(None, LevelFilter::Trace)
        .init();
}
