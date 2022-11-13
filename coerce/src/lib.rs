#[macro_use]
extern crate async_trait;

#[macro_use]
extern crate serde;

#[macro_use]
extern crate lazy_static;

#[macro_use]
extern crate metrics;

#[macro_use]
extern crate tracing;
extern crate core;

use std::fmt::Debug;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing_core::{Event, Interest, Metadata};
use tracing_subscriber::layer::Context;
use tracing_subscriber::Layer;

pub mod actor;
pub mod persistent;
pub mod remote;

pub(crate) const CARGO_PKG_VERSION: &str = env!("CARGO_PKG_VERSION");
