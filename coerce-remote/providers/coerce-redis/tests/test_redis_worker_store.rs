use coerce_redis::workers::RedisWorkerStore;
use coerce_redis::RedisWorker;

use coerce_rt::actor::context::ActorContext;

#[ignore] // ignored due to dependency on redis server
#[tokio::test]
pub async fn test_redis_worker_store() {
    let mut context = ActorContext::new();
    let redis = RedisWorker::new("127.0.0.1:6379".to_owned(), 4, &mut context)
        .await
        .unwrap();

    let _workers = RedisWorkerStore::new(&redis);
}
