use coerce::actor::context::ActorContext;
use coerce::actor::describe::Describe;
use coerce::actor::message::{Handler, Message};
use coerce::actor::system::ActorSystem;
use coerce::actor::{Actor, ActorId, CoreActorRef, IntoActor, IntoActorId};
use std::sync::Arc;
use tokio::sync::oneshot;

pub mod util;

#[macro_use]
extern crate tracing;

#[macro_use]
extern crate async_trait;

#[macro_use]
extern crate serde;

struct TestSupervisor {
    count: usize,
    max_depth: usize,
    all_child_actors_stopped: Option<oneshot::Sender<ActorId>>,
}

struct StopAll;

impl Message for StopAll {
    type Result = ();
}

#[async_trait]
impl Actor for TestSupervisor {
    async fn started(&mut self, ctx: &mut ActorContext) {
        for i in 0..self.count {
            let _child = ctx
                .spawn(
                    format!("spawned-{}", i).into_actor_id(),
                    SpawnedActor {
                        depth: 1,
                        max_depth: self.max_depth,
                    },
                )
                .await
                .unwrap();
        }
    }

    async fn on_child_stopped(&mut self, id: &ActorId, ctx: &mut ActorContext) {
        info!("child stopped (id={})", &id);

        if ctx.supervised_count() == 0 {
            if let Some(cb) = self.all_child_actors_stopped.take() {
                let _ = cb.send(id.clone());
            }
        }
    }
}

#[async_trait]
impl Handler<StopAll> for TestSupervisor {
    async fn handle(&mut self, _: StopAll, ctx: &mut ActorContext) {
        if let Some(supervised) = ctx.supervised() {
            for child in &supervised.children {
                let _ = child.1.actor_ref().stop().await;
            }
        }
    }
}

struct SpawnedActor {
    depth: usize,
    max_depth: usize,
}

#[async_trait]
impl Actor for SpawnedActor {
    async fn started(&mut self, ctx: &mut ActorContext) {
        if self.depth > self.max_depth {
            return;
        }

        let _child = ctx
            .spawn(
                "spawned".into_actor_id(),
                SpawnedActor {
                    depth: self.depth + 1,
                    max_depth: self.max_depth,
                },
            )
            .await
            .unwrap();
    }
}

#[tokio::test]
pub async fn test_actor_child_spawn_and_stop() {
    util::create_trace_logger();

    const EXPECTED_ACTOR_COUNT: usize = 10;
    const EXPECTED_DEPTH: usize = 10;
    let system = ActorSystem::new();
    let actor_id = "actor".to_string();

    let (all_child_actors_stopped, on_all_child_actors_stopped) = oneshot::channel();

    let test_actor = TestSupervisor {
        count: EXPECTED_ACTOR_COUNT,
        max_depth: EXPECTED_DEPTH,
        all_child_actors_stopped: Some(all_child_actors_stopped),
    }
    .into_actor(Some(actor_id), &system)
    .await
    .expect("create actor");

    let (tx, rx) = oneshot::channel();
    let _ = test_actor.describe(Describe {
        sender: Some(tx),
        current_depth: 0,
        ..Default::default()
    });

    let actor_description = rx.await.unwrap();
    info!("{:#?}", actor_description);

    let actor_count = actor_description
        .supervised
        .map_or(0, |s| s.actors.iter().filter(|n| n.is_ok()).count());

    assert_eq!(EXPECTED_ACTOR_COUNT, actor_count);

    let _ = test_actor.notify(StopAll);

    on_all_child_actors_stopped
        .await
        .expect("parent didn't receive the child-terminated notification");

    let (tx, rx) = oneshot::channel();
    let _ = test_actor.describe(Describe {
        sender: Some(tx),
        current_depth: 0,
        ..Default::default()
    });

    let x = rx.await;
    info!("{:#?}", x);

    system.shutdown().await;
}
