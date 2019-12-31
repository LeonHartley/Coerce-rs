use coerce_rt::actor::ActorId;

pub struct ActorState {
    actor_id: ActorId,
    state: Vec<u8>,
}

#[async_trait]
pub trait ActorStore {
    async fn get(&mut self, actor_id: ActorId);

    async fn put(&mut self, state: &ActorState);

    async fn remote(&mut self, actor_id: ActorId);
}
