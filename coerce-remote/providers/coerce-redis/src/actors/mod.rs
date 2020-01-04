use crate::{RedisWorkerErr, RedisWorkerRef, RedisWorkerRefExt};
use coerce_remote::storage::state::{ActorState, ActorStore, ActorStoreErr};
use uuid::Uuid;

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
    async fn get(&mut self, actor_id: Uuid) -> Result<Option<ActorState>, ActorStoreErr> {
        let key = actor_key(actor_id);
        let value: Option<Vec<u8>> = self.redis.command(resp_array!["GET", key]).await?;

        Ok(value.map(|state| ActorState { actor_id, state }))
    }

    async fn put(&mut self, actor: &ActorState) -> Result<(), ActorStoreErr> {
        let key = actor_key(actor.actor_id);

        self.redis
            .command(resp_array!["SET", key, actor.state.as_slice()])
            .await?;

        Ok(())
    }

    async fn remove(&mut self, actor_id: Uuid) -> Result<bool, ActorStoreErr> {
        let key = actor_key(actor_id);

        let deleted: Option<i32> = self.redis.command(resp_array!["DEL", key]).await?;
        Ok(match deleted {
            Some(n) if n > 0 => true,
            _ => false,
        })
    }
}

fn actor_key(id: Uuid) -> String {
    format!("coerce-actor-{}", id)
}

impl From<RedisWorkerErr> for ActorStoreErr {
    fn from(e: RedisWorkerErr) -> Self {
        ActorStoreErr::Other(format!("{:?}", e))
    }
}
