use crate::util::TestActor;
use async_trait::async_trait;
use coerce::actor::context::ActorContext;
use coerce::actor::message::Handler;
use coerce::actor::system::ActorSystem;
use coerce::actor::watch::{ActorTerminated, ActorWatch};
use coerce::actor::{Actor, ActorId, CoreActorRef, IntoActor, LocalActorRef};
use tokio::sync::oneshot;
use tokio::sync::oneshot::Sender;

pub mod util;



pub struct Watchdog {
    target: LocalActorRef<TestActor>,
    on_actor_terminated: Option<Sender<ActorTerminated>>,
}

#[async_trait]
impl Actor for Watchdog {
    async fn started(&mut self, ctx: &mut ActorContext) {
        self.watch(&self.target, ctx);
    }
}

#[async_trait]
impl Handler<ActorTerminated> for Watchdog {
    async fn handle(&mut self, message: ActorTerminated, _ctx: &mut ActorContext) {
        if let Some(on_actor_terminated) = self.on_actor_terminated.take() {
            let _ = on_actor_terminated.send(message);
        }
    }
}

#[tokio::test]
pub async fn test_actor_watch_notifications() {
    let system = ActorSystem::new();
    let actor = TestActor::new().into_actor(Option::<ActorId>::None, &system).await.unwrap();
    let (tx, rx) = oneshot::channel();
    let _watchdog = Watchdog {
        target: actor.clone(),
        on_actor_terminated: Some(tx),
    }
    .into_actor(Option::<ActorId>::None, &system)
    .await
    .unwrap();

    let _ = actor.notify_stop();
    let actor_terminated = rx.await.unwrap();
    let terminated_actor = actor_terminated.actor_ref();

    assert_eq!(terminated_actor.actor_id(), actor.actor_id());
    assert_eq!(terminated_actor.is_valid(), false);
}
