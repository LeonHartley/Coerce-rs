use bencher::{benchmark_group, benchmark_main, Bencher};
use coerce::actor::scheduler::ActorType::Anonymous;
use coerce::actor::system::ActorSystem;
use coerce::actor::{Actor, IntoActorId, LocalActorRef};
use tokio::runtime::Runtime;

fn rt() -> Runtime {
    tokio::runtime::Builder::new_multi_thread().build().unwrap()
}

fn create_1000_actors(bench: &mut Bencher) {
    let runtime = rt();

    bench.iter(|| {
        runtime.block_on(async {
            for _ in 0..1000 {
                let _ = actor().await;
            }
        })
    })
}

struct BenchmarkActor;

impl Actor for BenchmarkActor {}

async fn actor() -> LocalActorRef<BenchmarkActor> {
    let system = ActorSystem::new();
    system
        .new_actor("actor".into_actor_id(), BenchmarkActor, Anonymous)
        .await
        .expect("unable to create actor")
}

benchmark_group!(actor_creation, create_1000_actors);
benchmark_main!(actor_creation);
