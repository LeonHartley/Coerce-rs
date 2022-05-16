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

pub mod actor;
pub mod persistent;
pub mod remote;

pub(crate) const CARGO_PKG_VERSION: &str = env!("CARGO_PKG_VERSION");
