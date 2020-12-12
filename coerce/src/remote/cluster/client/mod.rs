use crate::actor::{Actor, ActorId, ActorRef, Factory};
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

    pub async fn create_actor<F: Factory>(
        &mut self,
        _recipe: F::Recipe,
        _id: Option<ActorId>,
    ) -> Option<ActorRef<F::Actor>> {
        None
    }

    pub async fn get_actor<A: Actor>(&mut self, _actor_id: ActorId) -> Option<ActorRef<A>>
    where
        A: 'static + Sync + Send,
    {
        None
    }
}
