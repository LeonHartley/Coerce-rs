use coerce_rt::actor::ActorId;

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct ActorState {
    pub actor_id: ActorId,
    pub state: Vec<u8>,
}

#[derive(Debug, Eq, PartialEq)]
pub enum ActorStoreErr {
    StoreUnavailable,
    InvalidConfig,
    Other(String),
}

#[async_trait]
pub trait ActorStore {
    async fn get(&mut self, actor_id: ActorId) -> Result<Option<ActorState>, ActorStoreErr>;

    async fn put(&mut self, actor: &ActorState) -> Result<(), ActorStoreErr>;

    async fn remove(&mut self, actor_id: ActorId) -> Result<bool, ActorStoreErr>;
}
