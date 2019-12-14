use crate::util::{
    GetCounterRequest, GetStatusRequest, GetStatusResponse, SetStatusRequest, SetStatusResponse,
    TestActor, TestActorStatus,
};
use coerce_rt::actor::context::{ActorContext, ActorHandlerContext};
use coerce_rt::actor::message::{Exec, Handler, Message, MessageResult};
use coerce_rt::actor::Actor;

#[macro_use]
extern crate async_trait;

pub mod util;

#[async_trait]
impl Handler<SetStatusRequest> for TestActor {
    async fn handle(
        &mut self,
        message: SetStatusRequest,
        _ctx: &mut ActorHandlerContext,
    ) -> SetStatusResponse {
        self.status = Some(message.0);

        SetStatusResponse::Ok
    }
}

#[async_trait]
impl Handler<GetCounterRequest> for TestActor {
    async fn handle(&mut self, _message: GetCounterRequest, _ctx: &mut ActorHandlerContext) -> i32 {
        self.counter
    }
}

#[async_trait]
impl Actor for TestActor {}

#[tokio::test]
pub async fn test_context_get_actor() {
    let ctx = ActorContext::new();
    let mut actor_ref = ctx.lock().unwrap().new_actor(TestActor::new());

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
