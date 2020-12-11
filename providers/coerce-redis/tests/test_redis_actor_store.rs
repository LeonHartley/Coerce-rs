use coerce_redis::actors::RedisActorStore;
use coerce_redis::RedisWorker;
use coerce::remote::storage::state::ActorStore;
use coerce::actor::context::ActorSystem;
use coerce::actor::ActorState;
use uuid::Uuid;

#[ignore] // ignored due to dependency on redis server
#[tokio::test]
pub async fn test_redis_actor_store() {
    let mut system = ActorSystem::new();
    let worker = RedisWorker::new("127.0.0.1:6379".to_owned(), 4, &mut system)
        .await
        .unwrap();

    let mut store = RedisActorStore::new(&worker);
    let actor_id = format!("{}", Uuid::new_v4());
    let state = ActorState {
        actor_id: actor_id.clone(),
        state: vec![1, 3, 3, 7],
    };

    assert_eq!(Ok(()), store.put(&state).await);
    assert_eq!(Ok(Some(state)), store.get(actor_id.clone()).await);
    assert_eq!(Ok(true), store.remove(actor_id.clone()).await);
    assert_eq!(Ok(None), store.get(actor_id).await);
}
