use coerce::actor::context::ActorContext;
use coerce::actor::system::ActorSystem;
use coerce::actor::{Actor, ActorId, IntoActor, IntoActorId};

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
        let child = ctx.spawn("child".into_actor_id(), ActorChild {}).await.unwrap();
        let _ = child.stop().await;
    }

    async fn on_child_stopped(&mut self, id: &ActorId, _ctx: &mut ActorContext) {
        info!("child terminated (id={})", &id);
        let _ = self.child_terminated_cb.take().unwrap().send(id.clone());
    }
}

struct ActorChild {}

impl Actor for ActorChild {}

#[tokio::test]
pub async fn test_actor_child_spawn_and_stop() {
    util::create_trace_logger();

    let system = ActorSystem::new();
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
