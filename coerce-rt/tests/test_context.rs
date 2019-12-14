use coerce_rt::actor::context::{ActorContext, ActorHandlerContext};
use coerce_rt::actor::Actor;
use util::TestActor;
use coerce_rt::actor::lifecycle::Status;

#[macro_use]
extern crate async_trait;

pub mod util;

#[async_trait]
impl Actor for TestActor {
    async fn started(&mut self, ctx: &mut ActorHandlerContext) {}

    async fn stopped(&mut self, ctx: &mut ActorHandlerContext) {}
}

#[tokio::test]
pub async fn test_actor_context_lifecycle() {
    let ctx = ActorContext::new();
    let mut actor_ref = ctx.lock().unwrap().new_actor(TestActor::new());

    let status = actor_ref.send(Status{}).await;

    assert_eq!(status)
}
