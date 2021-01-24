use actor::{Echo, EchoActor};
use coerce::actor::system::ActorSystem;
use coerce::actor::{Factory, IntoActor};
use coerce::remote::system::RemoteActorSystem;
use opentelemetry::global;
use opentelemetry::sdk::propagation::TraceContextPropagator;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

pub mod actor;

#[macro_use]
extern crate serde;

#[macro_use]
extern crate async_trait;

#[tokio::main]
pub async fn main() {
    global::set_text_map_propagator(TraceContextPropagator::new());

    let (tracer, _uninstall) = opentelemetry_jaeger::new_pipeline()
        .with_service_name("example-main")
        .install()
        .unwrap();
    let opentelemetry = tracing_opentelemetry::layer().with_tracer(tracer);
    tracing_subscriber::registry()
        .with(opentelemetry)
        .try_init()
        .unwrap();

    let mut system = ActorSystem::new();
    let mut remote = RemoteActorSystem::builder()
        .with_tag("example-main")
        .with_actor_system(system)
        .with_handlers(|handlers| handlers.with_handler::<EchoActor, Echo>("EchoActor.Echo"))
        .build()
        .await;

    remote
        .clone()
        .cluster_worker()
        .listen_addr("localhost:30100")
        .start()
        .await;

    let _ = EchoActor
        .into_actor(Some("echo-actor".to_string()), remote.inner())
        .await
        .expect("unable to start echo actor");

    tokio::signal::ctrl_c()
        .await
        .expect("failed to listen for event");
}
