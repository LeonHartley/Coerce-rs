use coerce::actor::context::ActorContext;
use coerce::actor::message::{Handler, Message};
use coerce::actor::Actor;
use coerce_macros::JsonMessage;

use coerce::actor::system::ActorSystem;

#[cfg(feature = "remote")]
use coerce::remote::system::{builder::RemoteSystemConfigBuilder, NodeId, RemoteActorSystem};

use std::str::FromStr;
use tracing::Level;
use tracing_subscriber::fmt::format::FmtSpan;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, Copy, Clone)]
pub enum TestActorStatus {
    Inactive,
    Active,
}

#[derive(JsonMessage, Serialize, Deserialize, Debug, Eq, PartialEq, Clone)]
#[result("GetStatusResponse")]
pub struct GetStatusRequest;

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, Clone)]
pub enum GetStatusResponse {
    Ok(TestActorStatus),
    None,
}

#[derive(JsonMessage, Serialize, Deserialize, Debug, Eq, PartialEq, Clone)]
#[result("SetStatusResponse")]
pub struct SetStatusRequest {
    pub status: TestActorStatus,
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, Clone)]
pub enum SetStatusResponse {
    Ok,
    Unsuccessful,
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

impl Actor for TestActor {}

#[derive(Serialize, Deserialize)]
pub struct EchoActor {}

impl EchoActor {
    pub fn new() -> EchoActor {
        EchoActor {}
    }
}

impl Actor for EchoActor {}

#[async_trait]
impl Handler<GetCounterRequest> for EchoActor {
    async fn handle(&mut self, _message: GetCounterRequest, _ctx: &mut ActorContext) -> i32 {
        42
    }
}

lazy_static::lazy_static! {
    static ref LOG_LEVEL: String = std::env::var("LOG_LEVEL").map_or(String::from("OFF"), |s| s);
}

#[cfg(feature = "remote")]
pub async fn create_cluster_node<F>(
    node_id: NodeId,
    listen_addr: &str,
    seed: Option<&str>,
    handlers: F,
) -> RemoteActorSystem
where
    F: 'static + (FnMut(&mut RemoteSystemConfigBuilder) -> &mut RemoteSystemConfigBuilder),
{
    let remote = RemoteActorSystem::builder()
        .with_actor_system(ActorSystem::new())
        .with_id(node_id)
        .with_handlers(handlers)
        .build()
        .await;

    let mut builder = remote.clone().cluster_worker().listen_addr(listen_addr);

    if let Some(seed) = seed {
        builder = builder.with_seed_addr(seed);
    }

    builder.start().await;

    remote
}

pub fn create_trace_logger() {
    let _ = tracing_subscriber::fmt()
        // enable everything
        .with_file(true)
        .with_line_number(true)
        .with_target(true)
        .with_thread_names(true)
        .with_span_events(FmtSpan::NONE)
        .with_ansi(false)
        .with_max_level(
            Level::from_str(LOG_LEVEL.as_str()).expect("invalid `LOG_LEVEL` environment variable"),
        )
        // sets this to be the default, global collector for this application.
        .try_init();
}

pub fn create_logger(level: Option<Level>) {
    let _ = tracing_subscriber::fmt()
        .compact()
        .with_thread_names(true)
        .with_span_events(FmtSpan::NONE)
        .with_ansi(false)
        .with_max_level(level.unwrap_or_else(|| {
            Level::from_str(LOG_LEVEL.as_str()).expect("invalid `LOG_LEVEL` environment variable")
        }))
        // sets this to be the default, global collector for this application.
        .try_init();
}
