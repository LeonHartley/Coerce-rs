// use coerce::actor::context::ActorContext;
// use coerce::actor::lifecycle::Status;
// use coerce::actor::message::{Handler, Message};
// use coerce::actor::scheduler::ActorType::Anonymous;
// use coerce::actor::system::ActorSystem;
// use coerce::actor::{Actor, IntoActor, IntoActorId, LocalActorRef};
// use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
//
// struct BenchmarkActor;
//
// impl Actor for BenchmarkActor {}
//
// struct Msg;
//
// impl Message for Msg {
//     type Result = ();
// }
//
// #[async_trait::async_trait]
// impl Handler<Msg> for BenchmarkActor {
//     async fn handle(&mut self, _message: Msg, _ctx: &mut ActorContext) {}
// }
//
// async fn actor_send_and_wait(
//     actor: LocalActorRef<BenchmarkActor>,
//     actor_2: LocalActorRef<BenchmarkActor>,
// ) {
//     let _ = futures::future::join_all(vec![actor.send(Msg), actor_2.send(Msg)]).await;
// }
//
// async fn actor_send_and_forget(actor: LocalActorRef<BenchmarkActor>) {
//     let _ = actor.notify(Msg).expect("");
// }
//
// fn actor_messaging_benchmark(c: &mut Criterion) {
//     let tokio_rt = tokio::runtime::Builder::new_multi_thread()
//         .worker_threads(3)
//         .build()
//         .unwrap();
//
//     let (actor, actor_2) = tokio_rt.block_on(async {
//         let system = ActorSystem::new();
//         (
//             system
//                 .new_actor(String::default().into_actor_id(), BenchmarkActor, Anonymous)
//                 .await
//                 .expect("unable to create actor"),
//             system
//                 .new_actor(String::default().into_actor_id(), BenchmarkActor, Anonymous)
//                 .await
//                 .expect("unable to create actor"),
//         )
//     });
//
//     c.bench_function("async", move |b| {
//         // Insert a call to `to_async` to convert the bencher to async mode.
//         // The timing loops are the same as with the normal bencher.
//         b.to_async(tokio_rt)
//             .iter(move || actor_send_and_wait(actor.clone(), actor_2.clone()));
//     });
//     //
//     // c.bench_function("actor send and wait", |b| b.iter(|| tokio_rt.block_on(actor_send_and_wait(actor.clone(), actor_2.clone()))));
//     // c.bench_function("actor send and forget", |b| b.iter(|| actor_send_and_forget(actor.clone())));
// }
//
// criterion_group!(benches, actor_messaging_benchmark);
// criterion_main!(benches);

fn main() {}
