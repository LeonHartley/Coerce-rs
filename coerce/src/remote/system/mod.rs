use chrono::{DateTime, Utc};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;

use crate::actor::system::ActorSystem;
use crate::actor::LocalActorRef;
use crate::remote::actor::{
    clients::RemoteClientRegistry, registry::RemoteRegistry, RemoteHandler,
};
use crate::remote::cluster::builder::client::ClusterClientBuilder;
use crate::remote::cluster::builder::worker::ClusterWorkerBuilder;
use crate::remote::cluster::discovery::NodeDiscovery;
use crate::remote::heartbeat::Heartbeat;
use crate::remote::stream::mediator::StreamMediator;
use crate::remote::system::builder::RemoteActorSystemBuilder;

pub mod actor;
pub mod builder;
pub mod cluster;
pub mod rpc;

use crate::remote::config::RemoteSystemConfig;
pub use actor::*;
pub use cluster::*;
pub use rpc::*;

#[derive(Clone)]
pub struct RemoteActorSystem {
    inner: Arc<RemoteSystemCore>,
}

pub type NodeId = u64;
pub type AtomicNodeId = AtomicI64;

#[derive(Clone)]
pub struct RemoteSystemCore {
    node_id: NodeId,
    inner: ActorSystem,
    started_at: DateTime<Utc>,
    handler_ref: Arc<parking_lot::Mutex<RemoteHandler>>,
    registry_ref: LocalActorRef<RemoteRegistry>,
    clients_ref: LocalActorRef<RemoteClientRegistry>,
    discovery_ref: LocalActorRef<NodeDiscovery>,
    heartbeat_ref: LocalActorRef<Heartbeat>,
    mediator_ref: Option<LocalActorRef<StreamMediator>>,
    config: Arc<RemoteSystemConfig>,
    current_leader: Arc<AtomicNodeId>,
    next_message_id: Arc<AtomicU64>,
}

impl RemoteActorSystem {
    pub async fn shutdown(&self) {
        self.inner.shutdown().await;
    }
}

impl RemoteSystemCore {
    pub async fn shutdown(&self) {
        let _ = self.heartbeat_ref.stop().await;
        let _ = self.clients_ref.stop().await;

        if let Some(mediator_ref) = self.mediator_ref.as_ref() {
            let _ = mediator_ref.stop().await;
        }

        let _ = self.discovery_ref.stop().await;
        let _ = self.registry_ref.stop().await;

        info!("shutdown complete");
    }
}

impl RemoteActorSystem {
    pub fn builder() -> RemoteActorSystemBuilder {
        RemoteActorSystemBuilder::new()
    }

    pub fn cluster_worker(self) -> ClusterWorkerBuilder {
        ClusterWorkerBuilder::new(self)
    }

    pub fn cluster_client(self) -> ClusterClientBuilder {
        ClusterClientBuilder::new(self)
    }

    pub fn config(&self) -> &RemoteSystemConfig {
        &self.inner.config
    }

    pub fn node_tag(&self) -> &str {
        self.inner.config.node_tag()
    }

    pub fn node_version(&self) -> &str {
        self.inner.config.node_version()
    }

    pub fn node_id(&self) -> NodeId {
        self.inner.node_id
    }

    pub fn started_at(&self) -> &DateTime<Utc> {
        &self.inner.started_at
    }

    pub fn heartbeat(&self) -> &LocalActorRef<Heartbeat> {
        &self.inner.heartbeat_ref
    }

    pub fn registry(&self) -> &LocalActorRef<RemoteRegistry> {
        &self.inner.registry_ref
    }

    pub fn client_registry(&self) -> &LocalActorRef<RemoteClientRegistry> {
        &self.inner.clients_ref
    }

    pub fn node_discovery(&self) -> &LocalActorRef<NodeDiscovery> {
        &self.inner.discovery_ref
    }

    pub fn stream_mediator(&self) -> Option<&LocalActorRef<StreamMediator>> {
        self.inner.mediator_ref.as_ref()
    }

    pub fn actor_system(&self) -> &ActorSystem {
        &self.inner.actor_system()
    }

    pub fn next_msg_id(&self) -> u64 {
        self.inner.next_message_id.fetch_add(1, Ordering::Relaxed)
    }
}

impl RemoteSystemCore {
    pub fn actor_system(&self) -> &ActorSystem {
        &self.inner
    }
}
