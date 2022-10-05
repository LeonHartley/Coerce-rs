use crate::remote::cluster::discovery::{Discover, Seed};
use crate::remote::cluster::node::RemoteNode;
use crate::remote::net::server::RemoteServer;
use crate::remote::system::RemoteActorSystem;
use tokio::sync::oneshot;

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
        // let span = tracing::trace_span!(
        //     "ClusterWorkerBuilder::start",
        //     listen_addr = self.server_listen_addr.as_str(),
        //     node_tag = self.system.node_tag()
        // );
        //
        // let _enter = span.enter();

        let started_at = *self.system.started_at();
        let cluster_node_addr = self.cluster_node_addr();
        self.system
            .register_node(RemoteNode::new(
                self.system.node_id(),
                cluster_node_addr.clone(),
                self.system.node_tag().to_string(),
                Some(started_at),
            ))
            .await;

        let server_ctx = self.system.clone();
        let mut server = RemoteServer::new();

        trace!(
            "starting on {}, external_addr={}",
            &self.server_listen_addr,
            &cluster_node_addr
        );

        let discover_peers = self.seed_addr.as_ref() != Some(&cluster_node_addr);

        server
            .start(
                self.server_listen_addr.clone(),
                cluster_node_addr,
                server_ctx,
            )
            .await
            .expect("failed to start server");

        if discover_peers {
            self.discover_peers().await;
        }

        server
    }

    fn cluster_node_addr(&self) -> String {
        self.server_external_addr
            .as_ref()
            .map_or_else(|| self.server_listen_addr.clone(), |s| s.clone())
    }

    async fn discover_peers(&mut self) {
        if let Some(seed_addr) = self.seed_addr.take() {
            // let span = tracing::trace_span!("ClusterWorkerBuilder::discover_peers");
            // let _enter = span.enter();

            let (tx, rx) = oneshot::channel();

            let _ = self.system.node_discovery().notify(Discover {
                seed: Seed::Addr(seed_addr.clone()),
                on_discovery_complete: Some(tx),
            });

            info!("discover_peers - waiting for discovery to complete");

            let _ = rx
                .await
                .expect(&format!("unable to discover nodes from addr={}", seed_addr));

            info!("discover_peers - discovered peers successfully");
        }
    }
}
