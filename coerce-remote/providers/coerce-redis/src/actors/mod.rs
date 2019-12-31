use coerce_remote::storage::state::{ActorState, ActorStore};
use crate::RedisWorkerRef;

pub struct RedisActorStore {
    redis: RedisWorkerRef,
}

