pub mod util;

#[macro_use]
extern crate serde;
extern crate serde_json;

extern crate chrono;

#[macro_use]
extern crate async_trait;

#[macro_use]
extern crate coerce_macros;

use coerce_remote::context::builder::RemoteActorHandlerBuilder;
use coerce_remote::context::RemoteActorContext;
use coerce_rt::actor::context::ActorContext;
use std::time::Duration;
use util::*;

#[coerce_test]
pub async fn test_remote_cluster_worker_builder() {
    util::create_trace_logger();

    let mut context = ActorContext::new();
    let actor = context.new_tracked_actor(TestActor::new()).await.unwrap();
    let remote = RemoteActorContext::builder()
        .with_actor_context(context)
        .with_handlers(build_handlers)
        .build()
        .await;

    let mut remote_c = remote.clone();

    let remote_2 = RemoteActorContext::builder()
        .with_actor_context(ActorContext::new())
        .with_handlers(build_handlers)
        .build()
        .await;

    let mut remote_2_c = remote_2.clone();

    let remote_3 = RemoteActorContext::builder()
        .with_actor_context(ActorContext::new())
        .with_handlers(build_handlers)
        .build()
        .await;

    let mut remote_3_c = remote_3.clone();

    remote
        .cluster_worker()
        .listen_addr("localhost:30101")
        .start()
        .await;

    remote_2
        .cluster_worker()
        .listen_addr("localhost:30102")
        .with_seed_addr("localhost:30101")
        .start()
        .await;

    remote_3
        .cluster_worker()
        .listen_addr("localhost:30103")
        .with_seed_addr("localhost:30101")
        .start()
        .await;

    let nodes_a = remote_c.get_nodes().await;
    let nodes_b = remote_2_c.get_nodes().await;
    let nodes_c = remote_3_c.get_nodes().await;

    log::info!("a: {:?}", &nodes_a);
    log::info!("b: {:?}", &nodes_b);
    log::info!("c: {:?}", &nodes_c);

    let nodes_a_in_b = nodes_a.iter().filter(|n| nodes_b.contains(n)).count();
    let nodes_a_in_c = nodes_a.iter().filter(|n| nodes_c.contains(n)).count();
    let nodes_b_in_a = nodes_b.iter().filter(|n| nodes_a.contains(n)).count();
    let nodes_b_in_c = nodes_b.iter().filter(|n| nodes_c.contains(n)).count();
    let nodes_c_in_a = nodes_c.iter().filter(|n| nodes_a.contains(n)).count();
    let nodes_c_in_b = nodes_c.iter().filter(|n| nodes_b.contains(n)).count();

    assert_eq!(nodes_a_in_b, nodes_a.len());
    assert_eq!(nodes_a_in_c, nodes_a.len());
    assert_eq!(nodes_b_in_a, nodes_b.len());
    assert_eq!(nodes_b_in_c, nodes_b.len());
    assert_eq!(nodes_c_in_a, nodes_c.len());
    assert_eq!(nodes_c_in_b, nodes_c.len());
}

fn build_handlers(handlers: &mut RemoteActorHandlerBuilder) -> &mut RemoteActorHandlerBuilder {
    handlers
        .with_handler::<TestActor, SetStatusRequest>("TestActor.SetStatusRequest")
        .with_handler::<TestActor, GetStatusRequest>("TestActor.GetStatusRequest")
        .with_handler::<EchoActor, GetCounterRequest>("EchoActor.GetCounterRequest")
}
