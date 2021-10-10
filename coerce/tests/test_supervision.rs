use coerce::actor::context::ActorContext;
use coerce::actor::system::ActorSystem;
use coerce::actor::{Actor, ActorId, IntoActor, LocalActorRef};
use std::any::Any;

pub mod util;

#[macro_use]
extern crate log;

#[macro_use]
extern crate async_trait;

#[macro_use]
extern crate serde;

struct TestActor {
    child_terminated_cb: Option<tokio::sync::oneshot::Sender<ActorId>>,
}

#[async_trait]
impl Actor for TestActor {
    async fn started(&mut self, ctx: &mut ActorContext) {
        let child = ctx.spawn("child".to_string(), ActorChild {}).await.unwrap();
        child.notify_stop();
    }

    async fn on_child_stopped(&mut self, id: &ActorId, ctx: &mut ActorContext) {
        info!("child terminated (id={})", &id);
        self.child_terminated_cb.take().unwrap().send(id.into());
    }
}

struct ActorChild {}

impl Actor for ActorChild {}

#[tokio::test]
pub async fn test_actor_child_spawn_and_stop() {
    util::create_trace_logger();

    let mut system = ActorSystem::new();
    let actor_id = "actor".to_string();

    let (child_terminated_cb, on_child_stopped) = tokio::sync::oneshot::channel();

    let _ = TestActor {
        child_terminated_cb: Some(child_terminated_cb),
    }
    .into_actor(Some(actor_id), &system)
    .await
    .expect("create actor");

    on_child_stopped
        .await
        .expect("parent didn't receive the child-terminated notification");
    system.shutdown().await;
}
