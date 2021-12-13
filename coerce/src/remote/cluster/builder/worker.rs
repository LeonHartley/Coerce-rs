use crate::remote::cluster::discovery::ClusterSeed;
use crate::remote::cluster::node::RemoteNode;
use crate::remote::net::client::ClientType::Worker;
use crate::remote::net::client::RemoteClient;
use crate::remote::net::server::RemoteServer;
use crate::remote::system::RemoteActorSystem;

use chrono::Utc;
use tokio::time::Duration;

pub struct ClusterWorkerBuilder {
    server_listen_addr: String,
    seed: Option<Box<dyn ClusterSeed + Send + Sync>>,
    seed_addr: Option<String>,
    system: RemoteActorSystem,
}

impl ClusterWorkerBuilder {
    pub fn new(system: RemoteActorSystem) -> ClusterWorkerBuilder {
        let server_listen_addr = "0.0.0.0:30101".to_owned();
        let seed = None;
        let seed_addr = None;

        ClusterWorkerBuilder {
            server_listen_addr,
            system,
            seed,
            seed_addr,
        }
    }

    pub fn with_seed<S: ClusterSeed>(mut self, seed: S) -> Self
    where
        S: 'static + Send + Sync,
    {
        self.seed = Some(Box::new(seed));

        self
    }

    pub fn with_seed_addr<T: ToString>(mut self, seed_addr: T) -> Self {
        self.seed_addr = Some(seed_addr.to_string());

        self
    }

    pub fn listen_addr<T: ToString>(mut self, listen_addr: T) -> Self {
        self.server_listen_addr = listen_addr.to_string();

        self
    }

    pub async fn start(mut self) {
        let span = tracing::trace_span!(
            "ClusterWorkerBuilder::start",
            listen_addr = self.server_listen_addr.as_str(),
            node_tag = self.system.node_tag()
        );

        let _enter = span.enter();

        self.system
            .register_node(RemoteNode::new(
                self.system.node_id(),
                self.server_listen_addr.clone(),
                Some(Utc::now()),
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

        // let node_id = self.system.node_id();
        // let sys = self.system.clone();

        // tokio::spawn(async move {
        //     let mut metrics = sys.raft().unwrap().core().metrics();
        //     tokio::spawn(async move {
        //         while metrics.changed().await.is_ok() {
        //             info!("raft changed (node={}): {:?}", node_id, *metrics.borrow());
        //         }
        //     });
        //
        //     tokio::time::sleep(Duration::from_secs(1)).await;
        //     let nodes = sys.get_nodes().await;
        //
        //     info!("registering nodes: {:?}, current={}", &nodes, node_id);
        //
        //     sys.raft()
        //         .unwrap()
        //         .core()
        //         .initialize(
        //             nodes
        //                 .iter()
        //                 .filter(|n| n.id != node_id)
        //                 .map(|n| n.id)
        //                 .collect(),
        //         )
        //         .await;
        // })
    }

    async fn discover_peers(&mut self) {
        if let Some(seed_addr) = self.seed_addr.take() {
            let span = tracing::trace_span!("ClusterWorkerBuilder::discover_peers");
            let enter = span.enter();

            let client_ctx = self.system.clone();
            let client = RemoteClient::connect(seed_addr.clone(), None, client_ctx, None, Worker)
                .await
                .expect("failed to connect to seed server");

            self.system
                .register_node(RemoteNode::new(
                    client.node_id,
                    seed_addr,
                    Some(client.node_started_at),
                ))
                .await;

            let node_id = client.node_id;
            self.system.register_client(node_id, client).await;

            drop(enter);
            drop(span);

            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
}
