use crate::remote::cluster::client::RemoteClusterClient;
use crate::remote::system::RemoteActorSystem;

pub struct ClusterClientBuilder {
    system: RemoteActorSystem,
    // seed_node:
}

impl ClusterClientBuilder {
    pub fn new(system: RemoteActorSystem) -> ClusterClientBuilder {
        ClusterClientBuilder { system }
    }

    pub fn build(self) -> RemoteClusterClient {
        RemoteClusterClient::new(self.system)
    }
}
