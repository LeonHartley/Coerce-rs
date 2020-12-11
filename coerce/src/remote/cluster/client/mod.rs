use crate::actor::{Actor, ActorId};
use crate::remote::context::RemoteActorSystem;
use crate::remote::RemoteActorRef;
use serde::de::DeserializeOwned;
use serde::Serialize;

#[derive(Clone)]
pub struct RemoteClusterClient {
    context: RemoteActorSystem,
}

impl RemoteClusterClient {
    pub fn new(context: RemoteActorSystem) -> RemoteClusterClient {
        RemoteClusterClient { context }
    }
}

impl RemoteClusterClient {
    pub async fn create_actor<A: Actor>(
        &mut self,
        id: Option<ActorId>,
        state: A,
    ) -> Option<RemoteActorRef<A>>
    where
        A: 'static + Sync + Send,
        A: Serialize + Sync + Send,
        A: DeserializeOwned + Sync + Send,
    {
        None
    }

    pub async fn get_actor<A: Actor>(&mut self, actor_id: ActorId) -> Option<RemoteActorRef<A>>
    where
        A: 'static + Sync + Send,
    {
        None
    }
}
