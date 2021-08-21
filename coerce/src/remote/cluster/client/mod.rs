use crate::actor::{Actor, ActorId, ActorRef};
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

impl RemoteClusterClient {
    pub fn actor_system(&self) -> &RemoteActorSystem {
        &self.system
    }
}
