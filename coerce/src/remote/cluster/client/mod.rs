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
    // pub async fn create_actor<A: Actor>(
    //     &mut self,
    //     id: Option<ActorId>,
    //     state: A,
    // ) -> Option<ActorRef<A>>
    // where
    //     A: 'static + Sync + Send,
    //     A: Serialize + Sync + Send,
    //     A: DeserializeOwned + Sync + Send,
    // {
    //     None
    // }

    pub async fn get_actor<A: Actor>(&mut self, _actor_id: ActorId) -> Option<ActorRef<A>>
    where
        A: 'static + Sync + Send,
    {
        None
    }
}
