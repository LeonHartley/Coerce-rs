#[macro_use]
extern crate async_trait;

#[macro_use]
extern crate serde;

#[macro_use]
extern crate lazy_static;

#[cfg(feature = "metrics")]
#[macro_use]
extern crate metrics;

#[macro_use]
extern crate tracing;

pub mod actor;

#[cfg(feature = "persistence")]
pub mod persistent;

#[cfg(feature = "remote")]
pub mod remote;

#[cfg(feature = "sharding")]
pub mod sharding;

pub(crate) const CARGO_PKG_VERSION: &str = env!("CARGO_PKG_VERSION");
