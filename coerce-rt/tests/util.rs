
use coerce_rt::actor::message::{Handler, Message, MessageResult};


pub struct TestActor {
    pub status: Option<TestActorStatus>,
    pub counter: i32,
}

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

impl TestActor {
    pub fn new() -> TestActor {
        TestActor {
            status: None,
            counter: 0,
        }
    }
}
