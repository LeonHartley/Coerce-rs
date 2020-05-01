use crate::cluster::client::RemoteClusterClient;
use crate::context::RemoteActorContext;

pub struct ClusterClientBuilder {
    context: RemoteActorContext,
}

impl ClusterClientBuilder {
    pub fn new(context: RemoteActorContext) -> ClusterClientBuilder {
        ClusterClientBuilder { context }
    }

    pub fn build(self) -> RemoteClusterClient {
        RemoteClusterClient::new(self.context)
    }
}
