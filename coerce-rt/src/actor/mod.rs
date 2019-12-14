use crate::actor::context::{ActorContext, ActorHandlerContext};
use crate::actor::message::{Handler, Message};
use crate::actor::scheduler::ActorRef;
use std::any::Any;
use std::marker::PhantomData;
use std::mem::transmute;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

pub mod context;
pub mod lifecycle;
pub mod message;
pub mod scheduler;

pub type ActorId = Uuid;

#[async_trait]
pub trait Actor {
    async fn started(&mut self, ctx: &mut ActorHandlerContext) {
        println!("actor started");
    }

    async fn stopped(&mut self, ctx: &mut ActorHandlerContext) {
        println!("actor stopped");
    }
}
