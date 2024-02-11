//! Coerce is a framework for creating distributed, event-driven applications, implementing [Carl Hewitt]'s [actor model].
//!
//! Taking inspiration from [akka], [akka.net], [actix] and [Orleans], Coerce brings
//! the power and flexibility of distributed actors and other industry-leading patterns to your Rust toolbox and
//! provides the required primitives for creating highly available, fault tolerant distributed applications.
//!
//! Coerce uses [tokio]'s asynchronous runtime and IO framework to ensure best-in-class performance
//! and concurrency, and seamless compatibility with other `async` libraries.
//!
//! Asynchronous traits are currently achieved by using the [async_trait] macro, with the intention
//! to drop the dependency once `async fn in trait` is generally available in stable Rust.
//!
//! [Carl Hewitt]: https://en.wikipedia.org/wiki/Carl_Hewitt
//! [actor model]: https://en.wikipedia.org/wiki/Actor_model
//! [akka]: https://github.com/akka/akka
//! [akka.net]: https://github.com/akkadotnet/akka.net
//! [actix]: https://github.com/actix/actix
//! [Orleans]: https://github.com/dotnet/orleans
//! [tokio]: https://github.com/tokio/tokio-rs
//! [async_trait]: https://github.com/dtolnay/async-trait
//!
//!
//! # Features
//! - [Asynchronous Actors]
//! - [Actor References]
//! - [Asynchronous Message Handling]
//! - [Actor Supervision]
//! - [Actor Persistence]
//!     - [Event sourcing]
//!     - [Snapshots]
//!     - [Pluggable Storage Backend]
//! - [Clustering]
//! - [Distributed PubSub]
//! - [Distributed Sharding]
//! - [Health Checks]
//! - [Actor Metrics]
//! - [Networking Metrics]
//!
//! [Asynchronous Actors]: crate::actor
//! [Actor References]: crate::actor::ActorRef
//! [Asynchronous Message Handling]: crate::actor::message
//! [Actor Supervision]: crate::actor::supervised
//! [Actor Persistence]: crate::persistent::actor::PersistentActor
//! [Event sourcing]: crate::persistent::actor::Recover
//! [Snapshots]: crate::persistent::actor::RecoverSnapshot
//! [Pluggable Storage Backend]: crate::persistent::journal::storage::JournalStorage
//! [Clustering]: crate::remote::cluster
//! [Distributed PubSub]: crate::remote::stream
//! [Distributed Sharding]: crate::sharding
//! [Health Checks]: crate::remote::heartbeat::health
//! [Actor Metrics]: crate::actor::metrics
//! [Networking Metrics]: crate::remote::net::metrics
//!
//! ## Feature Flags
//! Coerce is split into numerous features, which allows you to choose which Coerce features are to
//! be compiled, allowing you to speed up compilation if there are only specific modules your application uses:
//!
//! - `full` - Enables all features
//! - `remote` - Enables remoting and clustering
//! - `persistence` - Enables actor persistence
//! - `metrics` - Enables actor metrics + metrics exporter
//! - `sharding` - Enables distributed sharding
//! - `actor-tracing` - Enables actor tracing
//! - `api` - Enables HTTP API server
//! - `client-auth-jwt` - Enables JWT authentication between Coerce cluster nodes
//!
//! # Getting Started
//! The entry point into the Coerce runtime is an [`ActorSystem`]. Every [`ActorSystem`] has an ID
//! a name and an [`ActorScheduler`]. The [`ActorScheduler`] acts as a local registry and is
//! responsible for keeping track of actors, allowing retrieval of [`LocalActorRef`]s by [`ActorId`].
//!
//! Actors can be created by either using the [`IntoActor`] trait on instances of any type that implement
//! the [`Actor`] trait, or by calling `new_actor` on the [`ActorSystem`] directly.
//!
//! At a high level, there are 2 types of actors that can be created, [`Tracked`] and [`Anonymous`].
//! The only differences between them are that [`Tracked`] actors are registered with the
//! [`ActorScheduler`], which means their [`LocalActorRef`] can be retrieved and they can be communicated
//! with remotely, but an [`Anonymous`] actor cannot be retrieved and is not available for remote communication.
//!
//! [`ActorSystem`]: crate::actor::system::ActorSystem
//! [`ActorScheduler`]: crate::actor::scheduler::ActorScheduler
//! [`LocalActorRef`]: crate::actor::LocalActorRef
//! [`ActorId`]: crate::actor::ActorId
//! [`IntoActor`]: crate::actor::IntoActor
//! [`Actor`]: crate::actor::Actor
//! [`Tracked`]: crate::actor::scheduler::ActorType::Tracked
//! [`Anonymous`]: crate::actor::scheduler::ActorType::Anonymous
//!
//!
//! ## Actor Example
//! ```rust
//!
//! use coerce::actor::{
//!     Actor, IntoActor,
//!     message::{Message, Handler},
//!     context::ActorContext,
//!     system::ActorSystem
//! };
//!
//! #[tokio::main]
//! pub async fn main() {
//!     let system = ActorSystem::new();
//!     let actor = PlayerActor::default()
//!                 .into_actor(Some("player-actor-1"), &system)
//!                 .await
//!                 .unwrap();
//!
//!     let _ = actor.notify(PointsScored(1));
//!     let _ = actor.notify(PointsScored(2));
//!     let _ = actor.notify(PointsScored(3));
//!
//!     let points = actor.send(GetPoints).await.ok();
//!     assert_eq!(points, Some(6))
//! }
//!
//! #[derive(Default)]
//! struct PlayerActor {
//!     points: usize,
//! }
//!
//! impl Actor for PlayerActor { }
//!
//! pub struct PointsScored(usize);
//!
//! impl Message for PointsScored {
//!     type Result = ();
//! }
//!
//! pub struct GetPoints;
//!
//! impl Message for GetPoints {
//!     type Result = usize;
//! }
//!
//! #[async_trait::async_trait]
//! impl Handler<PointsScored> for PlayerActor {
//!     async fn handle(&mut self, message: PointsScored, ctx: &mut ActorContext) {
//!         self.points += message.0;
//!     }
//! }
//!
//! #[async_trait::async_trait]
//! impl Handler<GetPoints> for PlayerActor {
//!     async fn handle(&mut self, message: GetPoints, ctx: &mut ActorContext) -> usize {
//!         self.points
//!     }
//! }
//!
//! ```
//!

#[macro_use]
extern crate async_trait;

#[macro_use]
extern crate serde;

#[macro_use]
extern crate lazy_static;

#[cfg(feature = "metrics")]
#[macro_use]
extern crate metrics;

#[cfg(feature = "api")]
#[macro_use]
extern crate utoipa;

#[macro_use]
extern crate tracing;

pub mod actor;

#[cfg(feature = "persistence")]
pub mod persistent;

#[cfg(feature = "remote")]
pub mod remote;

#[cfg(feature = "sharding")]
pub mod sharding;
#[cfg(feature = "singleton")]
pub mod singleton;

pub(crate) const CARGO_PKG_VERSION: &str = env!("CARGO_PKG_VERSION");
