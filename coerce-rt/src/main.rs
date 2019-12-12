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

#[macro_use]
extern crate async_trait;
extern crate tokio;
extern crate uuid;

pub mod actor;
