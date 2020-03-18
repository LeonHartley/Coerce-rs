use crate::{RedisWorkerErr, RedisWorkerRef, RedisWorkerRefExt};
use coerce_remote::storage::state::{ActorState, ActorStore, ActorStoreErr};
use uuid::Uuid;
use coerce_rt::actor::ActorId;

pub struct RedisActorStore {
    redis: RedisWorkerRef,
}

impl RedisActorStore {
    pub fn new(redis: &RedisWorkerRef) -> RedisActorStore {
        let redis = redis.clone();
        RedisActorStore { redis }
    }
}

#[async_trait]
impl ActorStore for RedisActorStore {
    async fn get(&mut self, actor_id: ActorId) -> Result<Option<ActorState>, ActorStoreErr> {
        let key = actor_key(actor_id);
        let value: Option<Vec<u8>> = self.redis.command(resp_array!["GET", key.clone()]).await?;

        Ok(value.map(|state| ActorState { actor_id: key, state }))
    }

    async fn put(&mut self, actor: &ActorState) -> Result<(), ActorStoreErr> {
        let key = actor_key(actor.actor_id.clone());

        self.redis
            .command(resp_array!["SET", key, actor.state.as_slice()])
            .await?;

        Ok(())
    }

    async fn remove(&mut self, actor_id: ActorId) -> Result<bool, ActorStoreErr> {
        let key = actor_key(actor_id);

        let deleted: Option<i32> = self.redis.command(resp_array!["DEL", key]).await?;
        Ok(match deleted {
            Some(n) if n > 0 => true,
            _ => false,
        })
    }

    fn clone(&self) -> Box<dyn ActorStore + Send + Sync> {
        Box::new(RedisActorStore::new(&self.redis))
    }
}

fn actor_key(id: ActorId) -> String {
    format!("coerce-actor-{}", id)
}

impl From<RedisWorkerErr> for ActorStoreErr {
    fn from(e: RedisWorkerErr) -> Self {
        ActorStoreErr::Other(format!("{:?}", e))
    }
}
