use crate::remote::cluster::discovery::ClusterSeed;
use crate::remote::cluster::node::RemoteNode;
use crate::remote::codec::json::JsonCodec;
use crate::remote::net::client::RemoteClient;
use crate::remote::net::server::RemoteServer;
use crate::remote::system::RemoteActorSystem;

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
        let mut nodes = vec![];

        self.system
            .register_node(RemoteNode::new(
                self.system.node_id(),
                self.server_listen_addr.clone(),
            ))
            .await;

        if self.seed_addr.is_some() {
            self.discover_peers(&mut nodes).await;
        }

        let server_ctx = self.system.clone();
        let mut server = RemoteServer::new();

        server
            .start(self.server_listen_addr, server_ctx)
            .await
            .expect("failed to start server");
    }

    async fn discover_peers(&mut self, _nodes: &mut Vec<RemoteNode>) {
        if let Some(seed_addr) = self.seed_addr.take() {
            let client_ctx = self.system.clone();
            let client =
                RemoteClient::connect(seed_addr.clone(), client_ctx, None)
                    .await
                    .expect("failed to connect to seed server");

            self.system
                .register_node(RemoteNode::new(client.node_id, seed_addr))
                .await;
            self.system.register_client(client.node_id, client).await;
        }
    }
}
