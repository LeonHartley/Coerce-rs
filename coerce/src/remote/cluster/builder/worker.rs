use crate::remote::cluster::discovery::{Discover, Seed};
use crate::remote::cluster::node::RemoteNode;
use crate::remote::net::server::{RemoteServer, RemoteServerConfig};
use crate::remote::system::RemoteActorSystem;
use std::env;

use std::time::Duration;
use tokio::net::lookup_host;
use tokio::sync::oneshot;
use tokio::time::sleep;

pub struct ClusterWorkerBuilder {
    server_listen_addr: String,
    server_external_addr: Option<String>,
    seed_addr: Option<String>,
    system: RemoteActorSystem,
}

impl ClusterWorkerBuilder {
    pub fn new(system: RemoteActorSystem) -> ClusterWorkerBuilder {
        let server_listen_addr = "0.0.0.0:30101".to_owned();
        let seed_addr = None;
        let server_external_addr = None;
        ClusterWorkerBuilder {
            server_listen_addr,
            server_external_addr,
            system,
            seed_addr,
        }
    }

    pub fn with_seed_addr<T: ToString>(mut self, seed_addr: T) -> Self {
        self.seed_addr = Some(seed_addr.to_string());

        self
    }

    pub fn external_addr<T: ToString>(mut self, server_external_addr: T) -> Self {
        self.server_external_addr = Some(server_external_addr.to_string());
        self
    }

    pub fn listen_addr<T: ToString>(mut self, listen_addr: T) -> Self {
        self.server_listen_addr = listen_addr.to_string();

        self
    }

    pub async fn start(mut self) -> RemoteServer {
        let started_at = *self.system.started_at();
        let cluster_node_addr = self.cluster_node_addr();

        self.system
            .register_node(RemoteNode::new(
                self.system.node_id(),
                cluster_node_addr.clone(),
                self.system.node_tag().to_string(),
                Some(started_at),
                self.system.config().get_attributes().clone(),
            ))
            .await;

        let system = self.system.clone();
        let mut server = RemoteServer::new();

        trace!(
            "starting on {}, external_addr={}",
            &self.server_listen_addr,
            &cluster_node_addr
        );

        let listen_addr = self.server_listen_addr.clone();
        let override_incoming_node_addr = env::var("COERCE_OVERRIDE_INCOMING_NODE_ADDR")
            .map_or(false, |s| s == "1" || s.to_lowercase() == "true");

        let config = RemoteServerConfig::new(
            listen_addr,
            cluster_node_addr.clone(),
            override_incoming_node_addr,
        );

        server
            .start(config, system)
            .await
            .expect("failed to start server");

        if let Some(seed_addr) = self.seed_addr.take() {
            // TODO: this check only works if the listen addr & cluster node addr are equal,
            //        should we perform a resolution via `lookup_host` instead?

            if self.seed_addr.as_ref() != Some(&cluster_node_addr) {
                discover_peers(seed_addr, &self.system).await;
            }
        }

        server
    }

    fn cluster_node_addr(&self) -> String {
        self.server_external_addr
            .as_ref()
            .map_or_else(|| self.server_listen_addr.clone(), |s| s.clone())
    }
}

async fn discover_peers(seed_addr: String, system: &RemoteActorSystem) {
    const SEED_RESOLVE_MAX_ATTEMPTS: usize = 12;
    const SEED_RESOLVE_RETRY_DELAY: Duration = Duration::from_secs(5);

    // it's possible that the seed address DNS doesn't immediately resolve, but may resolve after a delay,
    // which is common with deployments using dynamic DNS such as Kubernetes deployments

    let mut attempts = 1;
    loop {
        if attempts >= SEED_RESOLVE_MAX_ATTEMPTS {
            error!(
                "Cannot resolve DNS for address: {} after 10 attempts, peer discovery cancelled",
                &seed_addr
            );
            return;
        }

        if seed_addr_resolves(&seed_addr).await {
            break;
        }

        warn!(
            "Cannot resolve DNS for seed address: {}, retrying in {}s",
            &seed_addr,
            &SEED_RESOLVE_RETRY_DELAY.as_secs()
        );

        sleep(SEED_RESOLVE_RETRY_DELAY).await;
        attempts += 1;
    }

    let (tx, rx) = oneshot::channel();

    let _ = system.node_discovery().notify(Discover {
        seed: Seed::Addr(seed_addr.clone()),
        on_discovery_complete: Some(tx),
    });

    info!("discovering cluster peers (seed={})", &seed_addr);

    let _ = rx
        .await
        .expect(&format!("unable to discover nodes from addr={}", seed_addr));

    info!("cluster peers discovered successfully");
}

async fn seed_addr_resolves(seed_addr: &str) -> bool {
    let resolves = lookup_host(seed_addr).await;
    resolves.is_ok()
}
