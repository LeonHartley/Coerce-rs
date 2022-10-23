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

#[derive(Default)]
pub struct TraceCollector {
    span_id: AtomicU64,
}

impl TraceCollector {
    fn next_span_id(&self) -> u64 {
        self.span_id.fetch_add(1, Ordering::Relaxed)
    }
}

impl<S> Layer<S> for TraceCollector
where
    S: tracing::Subscriber,
    S: Debug,
{
    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        println!("ctx => {:?}", ctx);
        println!("event => {:?}", event);
    }
}

pub(crate) const CARGO_PKG_VERSION: &str = env!("CARGO_PKG_VERSION");
