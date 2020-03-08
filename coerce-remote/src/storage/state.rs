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
