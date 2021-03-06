use coerce::actor::system::ActorSystem;
use coerce::actor::{Actor, IntoActor};
use std::error::Error;
use tracing::Instrument;
use tracing_subscriber::prelude::*;

struct TracingActor;

impl Actor for TracingActor {}

async fn app() {
    let mut sys = ActorSystem::new();

    for i in 0..10 {
        let actor_id = format!("actor-id-{}", i);
        tracing::info!(message = "starting actor", actor_id = actor_id.as_str());

        TracingActor
            .into_actor(Some(format!("actor-id-{}", i)), &mut sys)
            .await;
    }
}

#[tokio::test]
pub async fn test_tracing() -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    // tracing_subscriber::fmt()
    //     .with_max_level(tracing::Level::TRACE)
    //     // .with_span_events(FmtSpan::FULL)
    //     .try_init()?;

    let (tracer, _uninstall) = opentelemetry_jaeger::new_pipeline()
        .with_service_name("coerce")
        .install()?;
    let opentelemetry = tracing_opentelemetry::layer().with_tracer(tracer);
    tracing_subscriber::registry()
        .with(opentelemetry)
        .try_init()?;

    app().await;

    Ok(())
}
