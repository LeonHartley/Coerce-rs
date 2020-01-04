use crate::{RedisWorkerErr, RedisWorkerRef, RedisWorkerRefExt};
use coerce_remote::storage::state::{ActorState, ActorStore, ActorStoreErr};
use uuid::Uuid;

pub struct RedisActorStore {
    redis: RedisWorkerRef,
}

#[async_trait]
impl ActorStore for RedisActorStore {
    async fn get(&mut self, actor_id: Uuid) -> Result<Option<ActorState>, ActorStoreErr> {
        let value: Option<String> = self
            .redis
            .command(resp_array!["GET", actor_key(actor_id)])
            .await?;

        Err(ActorStoreErr::Other("hi".into()))
    }

    async fn put(&mut self, state: &ActorState) -> Result<(), ActorStoreErr> {
        unimplemented!()
    }

    async fn remove(&mut self, actor_id: Uuid) -> Result<(), ActorStoreErr> {
        unimplemented!()
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
