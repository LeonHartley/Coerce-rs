use coerce_rt::actor::ActorId;

pub struct ActorState {
    actor_id: ActorId,
    state: Vec<u8>,
}

#[derive(Debug)]
pub enum ActorStoreErr {
    StoreUnavailable,
    InvalidConfig,
    Other(String),
}

#[async_trait]
pub trait ActorStore {
    async fn get(&mut self, actor_id: ActorId) -> Result<Option<ActorState>, ActorStoreErr>;

    async fn put(&mut self, state: &ActorState) -> Result<(), ActorStoreErr>;

    async fn remove(&mut self, actor_id: ActorId) -> Result<(), ActorStoreErr>;
}
