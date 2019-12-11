use crate::actor::context::ActorContext;
use crate::actor::message::{HandleFuture, Handler, Message};
use crate::actor::scheduler::ActorScheduler;
use crate::actor::Actor;
use std::collections::HashMap;
use std::error::Error;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

extern crate tokio;
extern crate uuid;

pub mod actor;

pub struct LoginRequest {}

#[derive(Debug)]
pub enum LoginResponse {
    Ok,
    Unauthorised,
}

impl Message for LoginRequest {
    type Result = LoginResponse;
}

pub struct TestActor {}

impl Actor for TestActor {}

impl Handler<LoginRequest> for TestActor {
    fn handle(&self, message: LoginRequest) -> HandleFuture<LoginResponse> {
        Box::pin(async {
            println!("yooo!");

            LoginResponse::Ok
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut ctx = ActorContext::new();
    let mut addr = ctx.lock().unwrap().new_actor(TestActor {});
    let mut addr1 = ctx.lock().unwrap().new_actor(TestActor {});

    let res = addr.send(LoginRequest {}).await;

    println!("{:?}", res);
    Ok(())
}
