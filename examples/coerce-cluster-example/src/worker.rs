pub mod actor;

use crate::actor::{Echo, EchoActor};
use coerce::actor::system::ActorSystem;
use coerce::remote::system::RemoteActorSystem;
use opentelemetry::global;
use opentelemetry::sdk::propagation::TraceContextPropagator;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

#[macro_use]
extern crate serde;

#[macro_use]
extern crate async_trait;

#[tokio::main]
pub async fn main() {
    global::set_text_map_propagator(TraceContextPropagator::new());

    let (tracer, _uninstall) = opentelemetry_jaeger::new_pipeline()
        .with_service_name("example-worker")
        .install()
        .unwrap();

    let opentelemetry = tracing_opentelemetry::layer().with_tracer(tracer);
    tracing_subscriber::registry()
        .with(opentelemetry)
        .try_init()
        .unwrap();

    let mut system = ActorSystem::new();
    let mut remote = RemoteActorSystem::builder()
        .with_tag("example-worker")
        .with_actor_system(system)
        .with_handlers(|handlers| handlers.with_handler::<EchoActor, Echo>("EchoActor.Echo"))
        .build()
        .await;

    remote
        .clone()
        .cluster_worker()
        .listen_addr("localhost:30101")
        .with_seed_addr("localhost:30100")
        .start()
        .await;

    {
        let span = tracing::info_span!("CreateAndSend");
        let _enter = span.enter();

        let mut actor = remote
            .actor_ref::<EchoActor>("echo-actor".to_string())
            .await
            .expect("unable to get echo actor");

        let result = actor.send(Echo("hello".to_string())).await;
        assert_eq!(result.unwrap(), "hello".to_string());
    }

    tokio::signal::ctrl_c()
        .await
        .expect("failed to listen for event");
}
