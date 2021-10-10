use crate::remote::cluster::client::RemoteClusterClient;
use crate::remote::cluster::node::RemoteNode;
use crate::remote::net::client::ClientType::Client;
use crate::remote::net::client::RemoteClient;
use crate::remote::system::RemoteActorSystem;

pub struct ClusterClientBuilder {
    system: RemoteActorSystem,
    seed_addr: Option<String>,
}

impl ClusterClientBuilder {
    pub fn new(system: RemoteActorSystem) -> ClusterClientBuilder {
        ClusterClientBuilder {
            system,
            seed_addr: None,
        }
    }

    pub fn with_seed_addr<T: ToString>(mut self, seed_addr: T) -> Self {
        self.seed_addr = Some(seed_addr.to_string());

        self
    }

    pub async fn start(self) -> RemoteClusterClient {
        let seed_addr = self.seed_addr.expect("no seed addr");
        let system = self.system.clone();
        let client = RemoteClient::connect(seed_addr.clone(), None, system, None, Client)
            .await
            .expect("failed to connect to seed server");

        self.system
            .register_node(RemoteNode::new(client.node_id, seed_addr))
            .await;

        self.system.register_client(client.node_id, client).await;
        RemoteClusterClient::new(self.system)
    }
}
