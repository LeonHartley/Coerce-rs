use crate::actor::context::ActorContext;
use crate::actor::message::{HandleFuture, Handler, Message};
use std::any::Any;
use std::marker::PhantomData;
use std::mem::transmute;
use std::sync::{Arc, Mutex};
use uuid::Uuid;
use crate::actor::scheduler::ActorRef;

pub mod context;
pub mod message;
pub mod scheduler;

pub type ActorId = Uuid;

#[async_trait]
pub trait Actor {
    async fn started(&mut self) {
        println!("actor started");
    }

    async fn stopped(&mut self) {
        println!("actor stopped");
    }
}