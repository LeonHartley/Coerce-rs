use crate::actor::context::ActorContext;
use crate::actor::message::{HandleFuture, Handler, Message};
use std::any::Any;
use std::marker::PhantomData;
use std::mem::transmute;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

pub mod context;
pub mod message;
pub mod scheduler;

pub type ActorId = Uuid;

pub trait Actor {
    fn started(&mut self) -> HandleFuture<()> {
        Box::pin(async {})
    }

    fn stopped(&mut self) -> HandleFuture<()> {
        Box::pin(async {})
    }
}
