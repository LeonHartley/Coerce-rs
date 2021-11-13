use crate::remote::system::RemoteActorSystem;

#[derive(Clone)]
pub struct RemoteClusterClient {
    system: RemoteActorSystem,
}

impl RemoteClusterClient {
    pub fn new(system: RemoteActorSystem) -> RemoteClusterClient {
        RemoteClusterClient { system }
    }
}

impl RemoteClusterClient {}
