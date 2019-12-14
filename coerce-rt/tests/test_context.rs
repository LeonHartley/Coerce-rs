use crate::util::{
    GetCounterRequest, GetStatusRequest, GetStatusResponse, SetStatusRequest, SetStatusResponse,
    TestActor, TestActorStatus,
};
use coerce_rt::actor::context::{ActorContext, ActorHandlerContext};
use coerce_rt::actor::message::{Exec, Handler, Message, MessageResult};
use coerce_rt::actor::Actor;
use coerce_rt::actor::scheduler::ActorScheduler;
use coerce_rt::actor::scheduler::actor::RegisterActor;

#[macro_use]
extern crate async_trait;

pub mod util;

#[async_trait]
impl Actor for TestActor {}

#[tokio::test]
pub async fn test_context_get_actor() {
    let mut scheduler = ActorScheduler::new();
    let mut actor_ref = scheduler.send(RegisterActor(TestActor::new())).await;

    let _ = actor_ref
        .exec(|mut actor| {
            actor.counter = 1337;
        })
        .await;

    let mut actor = ctx
        .lock()
        .unwrap()
        .get_actor::<TestActor>(actor_ref.id)
        .unwrap();

    let counter = actor.exec(|actor| actor.counter).await;

    assert_eq!(counter, Ok(1337));
}
