use chrono::Local;
use coerce::actor::context::ActorContext;
use coerce::actor::message::encoding::json::RemoteMessage;
use coerce::actor::message::{Handler, Message};
use coerce::actor::Actor;
use env_logger::Builder;
use log::LevelFilter;
use std::io::Write;

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, Clone)]
pub enum TestActorStatus {
    Inactive,
    Active,
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, Clone)]
pub struct GetStatusRequest();

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, Clone)]
pub enum GetStatusResponse {
    Ok(TestActorStatus),
    None,
}

impl RemoteMessage for GetStatusRequest {
    type Result = GetStatusResponse;
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, Clone)]
pub struct SetStatusRequest {
    pub status: TestActorStatus,
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, Clone)]
pub enum SetStatusResponse {
    Ok,
    Unsuccessful,
}

impl RemoteMessage for SetStatusRequest {
    type Result = SetStatusResponse;
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, Clone)]
pub struct GetCounterRequest();

impl Message for GetCounterRequest {
    type Result = i32;
}

#[derive(Serialize, Deserialize)]
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
        _ctx: &mut ActorContext,
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
        _ctx: &mut ActorContext,
    ) -> SetStatusResponse {
        self.status = Some(message.status);

        SetStatusResponse::Ok
    }
}

#[async_trait]
impl Handler<GetCounterRequest> for TestActor {
    async fn handle(&mut self, _message: GetCounterRequest, _ctx: &mut ActorContext) -> i32 {
        42
    }
}

#[async_trait]
impl Actor for TestActor {}

#[derive(Serialize, Deserialize)]
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
    async fn handle(&mut self, _message: GetCounterRequest, _ctx: &mut ActorContext) -> i32 {
        42
    }
}

pub fn create_trace_logger() {
    Builder::new()
        .format(|buf, record| {
            writeln!(
                buf,
                "{} [{}] {} - {}",
                Local::now().format("%Y-%m-%dT%H:%M:%S%.6f"),
                record.level(),
                record.target(),
                record.args(),
            )
        })
        .filter(None, LevelFilter::Trace)
        .init();
}
