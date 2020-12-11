use crate::remote::cluster::client::RemoteClusterClient;
use crate::remote::context::RemoteActorSystem;

pub struct ClusterClientBuilder {
    context: RemoteActorSystem,
}

impl ClusterClientBuilder {
    pub fn new(context: RemoteActorSystem) -> ClusterClientBuilder {
        ClusterClientBuilder { context }
    }

    pub fn build(self) -> RemoteClusterClient {
        RemoteClusterClient::new(self.context)
    }
}
