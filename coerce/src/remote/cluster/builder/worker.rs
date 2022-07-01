use crate::remote::cluster::discovery::{Discover, Seed};
use crate::remote::cluster::node::RemoteNode;
use crate::remote::net::server::RemoteServer;
use crate::remote::system::RemoteActorSystem;
use tokio::sync::oneshot;

pub struct ClusterWorkerBuilder {
    server_listen_addr: String,
    seed_addr: Option<String>,
    system: RemoteActorSystem,
}

impl ClusterWorkerBuilder {
    pub fn new(system: RemoteActorSystem) -> ClusterWorkerBuilder {
        let server_listen_addr = "0.0.0.0:30101".to_owned();
        let seed_addr = None;

        ClusterWorkerBuilder {
            server_listen_addr,
            system,
            seed_addr,
        }
    }

    pub fn with_seed_addr<T: ToString>(mut self, seed_addr: T) -> Self {
        self.seed_addr = Some(seed_addr.to_string());

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
        self.system
            .register_node(RemoteNode::new(
                self.system.node_id(),
                self.server_listen_addr.clone(),
                self.system.node_tag().to_string(),
                Some(started_at),
            ))
            .await;

        let server_ctx = self.system.clone();
        let mut server = RemoteServer::new();

        trace!("starting on {}", &self.server_listen_addr);

        server
            .start(self.server_listen_addr.clone(), server_ctx)
            .await
            .expect("failed to start server");

        self.discover_peers().await;

        server
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
