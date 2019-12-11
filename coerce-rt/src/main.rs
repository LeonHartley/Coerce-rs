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

pub struct StatusRequest {}

#[derive(Debug)]
pub enum StatusResponse {
    Ok(PlayerStatus),
    Unauthorised,
}

impl Message for StatusRequest {
    type Result = StatusResponse;
}

#[derive(Debug)]
pub enum PlayerStatus {
    Idle,
    Active,
}

pub struct TestActor {
    status: PlayerStatus,
    i: i32,
}

impl TestActor {
    pub fn new() -> TestActor {
        TestActor {
            status: PlayerStatus::Idle,
            i: 0,
        }
    }
}

impl Actor for TestActor {}

impl Handler<LoginRequest> for TestActor {
    fn handle_sync(&mut self, message: LoginRequest) -> LoginResponse {
        self.i = self.i + 1;
        self.status = PlayerStatus::Active;
        println!("player is now active! {}", self.i);
        LoginResponse::Ok
    }
}

impl Handler<StatusRequest> for TestActor {
    fn handle_sync(&mut self, message: StatusRequest) -> StatusResponse {
        StatusResponse::Ok(match self.status {
            PlayerStatus::Active => PlayerStatus::Active,
            PlayerStatus::Idle => PlayerStatus::Idle,
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut ctx = ActorContext::new();
    let mut addr = ctx.lock().unwrap().new_actor(TestActor::new());
    let mut addr1 = ctx.lock().unwrap().new_actor(TestActor::new());

    loop {
        let player_status = addr.send(StatusRequest {}).await;
        let res = addr.send(LoginRequest {}).await;
    }

    let player_status = addr.send(StatusRequest {}).await;

    //    println!("{:?}", res);
    println!("{:?}", player_status);
    Ok(())
}
