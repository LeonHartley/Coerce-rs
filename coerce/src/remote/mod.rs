//! Coerce Remoting
//!
//! Coerce clusters are identified by a [`NodeId`] and a string-based node tag.
//!
//! The easiest way to create a full, batteries-included clustered actor system, is by using the [`RemoteActorSystemBuilder`].
//! From here, you can configure and create a [`RemoteActorSystem`].
//!
//! ## Remote-enabled messages
//! Messages are not available to be handled remotely by default, and do require being registered
//! with the [`RemoteActorSystem`] during the creation process.
//!
//! All message handlers must have a unique identifier assigned.
//!
//! ## Builder Example
//! ```rust
//! use coerce::actor::system::ActorSystem;
//! use coerce::remote::system::RemoteActorSystem;
//!
//! #[tokio::main]
//! async fn main() {
//!     let system = ActorSystem::new();
//!     let remote = RemoteActorSystem::builder()
//!         .with_id(1)
//!         .with_tag("node-name")
//!         .with_actor_system(system)
//!         .build()
//!         .await;
//! }
//! ```
//!
//! ## Registering Remote-enabled Messages
//! Remote-enabled messages can be enabled when creating a [`RemoteActorSystem`], below is an example
//! of a message of type `MyMessageType` registered, with the actor that processes the
//! message as type `MyActorType`.
//!
//! ```rust,no_run
//! use coerce::remote::system::RemoteActorSystem;
//!
//! let remote_system = RemoteActorSystem::builder()
//!      .with_id(1)
//!      .with_tag("node_name")
//!      .with_actor_system(system)
//!      .with_handlers(|handlers| {
//!          handlers
//!              .with_handler::<MyActorType, MyMessageType>("MyActorType.MyMessageType")
//!       });
//! ```
//!
//! [`NodeId`]: system::NodeId
//! [`RemoteActorSystemBuilder`]: system::builder::RemoteActorSystemBuilder
//! [`RemoteActorSystem`]: system::RemoteActorSystem

pub mod actor;
pub mod actor_ref;
pub mod cluster;
pub mod config;
pub mod handler;
pub mod heartbeat;
pub mod net;
pub mod raft;
pub mod stream;
pub mod system;
pub mod tracing;

#[cfg(feature = "api")]
pub mod api;

pub use actor_ref::*;
