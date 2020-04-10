use crate::context::RemoteActorContext;
use coerce_rt::actor::{ActorId, ActorState};


#[derive(Debug, Eq, PartialEq)]
pub enum ActorStoreErr {
    StoreUnavailable,
    InvalidConfig,
    Other(String),
}

impl ActorStoreErr {
    pub fn from_str(s: impl ToString) -> ActorStoreErr {
        ActorStoreErr::Other(s.to_string())
    }
}

#[async_trait]
pub trait ActorStore {
    async fn get(&mut self, actor_id: ActorId) -> Result<Option<ActorState>, ActorStoreErr>;

    async fn put(&mut self, actor: &ActorState) -> Result<(), ActorStoreErr>;

    async fn remove(&mut self, actor_id: ActorId) -> Result<bool, ActorStoreErr>;

    fn clone(&self) -> Box<dyn ActorStore + Sync + Send>;
}

#[async_trait]
pub trait StatefulActor {
    async fn save(&mut self, _ctx: RemoteActorContext) {
        trace!(target: "StatefulActor", "attempting to save actor state");
    }
}

// #[async_trait]
// impl<A: Actor> StatefulActor for A
// where
//     A: TryTo<ActorState>,
//     A: TryFrom<ActorState>,
// {
//
// }

// pub trait TryTo<T> {
//     type Error;
//
//     fn try_to(&self) -> Result<T, Self::Error>;
// }
//
// impl<A: Actor> TryFrom<ActorState> for A where A: DeserializeOwned {
//     type Error = ();
//
//     fn try_from(value: ActorState) -> Result<Self, Self::Error> {
//         match serde_json::from_slice(&value.state) {
//             Ok(a) => Ok(a),
//             _ => Err(())
//         }
//     }
// }
//
// impl<A: Actor> TryTo<ActorState> for A where A: Serialize {
//     type Error = ();
//
//     fn try_to(&self) -> Result<ActorState, Self::Error> {
//         let state = serde_json::to_string(&self);
//         if let Ok(state) = state {
//             Err(())
//         } else {
//             Err(())
//         }
//     }
// }
