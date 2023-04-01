//! These benchmarks exist as a way to find performance regressions, not as a demonstration of real world
//! performance

#[macro_use]
extern crate bencher;

use bencher::Bencher;
use coerce::actor::context::ActorContext;
use coerce::actor::message::{Handler, Message};
use coerce::actor::scheduler::ActorType::Anonymous;
use coerce::actor::system::ActorSystem;
use coerce::actor::{Actor, IntoActorId, LocalActorRef, ToActorId};
use tokio::runtime::Runtime;

struct BenchmarkActor;

impl Actor for BenchmarkActor {}

struct Msg;

impl Message for Msg {
    type Result = ();
}

#[async_trait::async_trait]
impl Handler<Msg> for BenchmarkActor {
    async fn handle(&mut self, _message: Msg, _ctx: &mut ActorContext) {}
}

async fn actor_1000_send_and_wait(actor: &LocalActorRef<BenchmarkActor>) {
    for _ in 0..1000 {
        let _ = actor.send(Msg).await.unwrap();
    }
}

async fn actor_999_notify_1_send_and_wait(actor: &LocalActorRef<BenchmarkActor>) {
    for _ in 0..999 {
        let _ = actor.notify(Msg);
    }

    let _ = actor.send(Msg).await.unwrap();
}

fn actor_send_1000_benchmark(bench: &mut Bencher) {
    let runtime = rt();
    let actor = runtime.block_on(async { actor().await });

    bench.iter(|| runtime.block_on(actor_1000_send_and_wait(&actor)));
}

fn actor_notify_1000_benchmark(bench: &mut Bencher) {
    let runtime = rt();
    let actor = runtime.block_on(async { actor().await });

    bench.iter(|| runtime.block_on(actor_999_notify_1_send_and_wait(&actor)))
}

fn rt() -> Runtime {
    tokio::runtime::Builder::new_multi_thread().build().unwrap()
}

async fn actor() -> LocalActorRef<BenchmarkActor> {
    let system = ActorSystem::new();
    system
        .new_actor("actor".into_actor_id(), BenchmarkActor, Anonymous)
        .await
        .expect("unable to create actor")
}

benchmark_group!(
    actor_messaging,
    actor_send_1000_benchmark,
    actor_notify_1000_benchmark
);
benchmark_main!(actor_messaging);
