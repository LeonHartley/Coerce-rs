use coerce_rt::actor::context::{ActorSystem, ActorContext};
use coerce_rt::actor::message::{Handler, Message};
use coerce_rt::actor::Actor;
use coerce_rt::worker::{Worker, WorkerRefExt};

#[macro_use]
extern crate async_trait;

#[derive(Clone)]
pub struct MyWorker {}

pub struct HeavyTask;

impl Actor for MyWorker {}

impl Message for HeavyTask {
    type Result = &'static str;
}

#[async_trait]
impl Handler<HeavyTask> for MyWorker {
    async fn handle(
        &mut self,
        _message: HeavyTask,
        _ctx: &mut ActorContext,
    ) -> &'static str {
        // do some IO with a connection pool attached to `MyWorker`?

        "my_result"
    }
}

#[tokio::test]
pub async fn test_workers() {
    let mut system = ActorSystem::new();

    let state = MyWorker {};
    let mut worker = Worker::new(state, 4, &mut context).await.unwrap();

    assert_eq!(worker.dispatch(HeavyTask).await, Ok("my_result"));
}
