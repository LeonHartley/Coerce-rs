use coerce::actor::context::ActorContext;
use coerce::actor::message::describe::Describe;
use coerce::actor::message::{Handler, Message};
use coerce::actor::system::ActorSystem;
use coerce::actor::{Actor, ActorId, CoreActorRef, IntoActor, IntoActorId};
use std::sync::Arc;
use tokio::sync::oneshot;

pub mod util;

#[macro_use]
extern crate log;

#[macro_use]
extern crate async_trait;

#[macro_use]
extern crate serde;

struct TestActor {
    all_child_actors_stopped: Option<tokio::sync::oneshot::Sender<ActorId>>,
}

struct StopAll;

impl Message for StopAll {
    type Result = ();
}

#[async_trait]
impl Actor for TestActor {
    async fn started(&mut self, ctx: &mut ActorContext) {
        for i in 0..100 {
            let _child = ctx
                .spawn(
                    format!("child-{}", i).into_actor_id(),
                    ActorChild { depth: 1 },
                )
                .await
                .unwrap();
        }
    }

    async fn on_child_stopped(&mut self, id: &ActorId, ctx: &mut ActorContext) {
        info!("child terminated (id={})", &id);

        if ctx.supervised_count() == 0 {
            if let Some(cb) = self.all_child_actors_stopped.take() {
                let _ = cb.send(id.clone());
            }
        }
    }
}

#[async_trait]
impl Handler<StopAll> for TestActor {
    async fn handle(&mut self, message: StopAll, ctx: &mut ActorContext) {
        if let Some(supervised) = ctx.supervised() {
            for child in &supervised.children {
                let _ = child.1.actor_ref().stop().await;
            }
        }
    }
}

struct ActorChild {
    depth: usize,
}

#[async_trait]
impl Actor for ActorChild {
    async fn started(&mut self, ctx: &mut ActorContext) {
        if self.depth > 10 {
            return;
        }

        let _child = ctx
            .spawn(
                "child".into_actor_id(),
                ActorChild {
                    depth: self.depth + 1,
                },
            )
            .await
            .unwrap();
    }
}

#[tokio::test]
pub async fn test_actor_child_spawn_and_stop() {
    util::create_trace_logger();

    let system = ActorSystem::new();
    let actor_id = "actor".to_string();

    let (all_child_actors_stopped, on_all_child_actors_stopped) = tokio::sync::oneshot::channel();

    let test_actor = TestActor {
        all_child_actors_stopped: Some(all_child_actors_stopped),
    }
    .into_actor(Some(actor_id), &system)
    .await
    .expect("create actor");

    let (tx, rx) = oneshot::channel();
    let _ = test_actor.describe(Describe {
        options: Arc::new(Default::default()),
        sender: Some(tx),
        current_depth: 0,
    });

    let x = rx.await;
    info!("{:#?}", x);

    let _ = test_actor.notify(StopAll);

    on_all_child_actors_stopped
        .await
        .expect("parent didn't receive the child-terminated notification");

    let (tx, rx) = oneshot::channel();
    let _ = test_actor.describe(Describe {
        options: Arc::new(Default::default()),
        sender: Some(tx),
        current_depth: 0,
    });

    let x = rx.await;
    info!("{:#?}", x);

    system.shutdown().await;
}
