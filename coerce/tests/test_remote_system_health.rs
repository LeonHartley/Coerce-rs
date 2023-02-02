use async_trait::async_trait;
use coerce::actor::context::ActorContext;
use coerce::actor::message::{Handler, Message};
use coerce::actor::system::ActorSystem;
use coerce::actor::{Actor, IntoActor};
use coerce::remote::heartbeat::health::HealthStatus;
use coerce::remote::heartbeat::Heartbeat;
use coerce::remote::system::RemoteActorSystem;

use std::time::Duration;

#[tokio::test]
pub async fn test_heartbeat_actor_monitoring() {
    const VERSION: &str = "1.0.0";
    let actor_system = ActorSystem::builder().system_name("heartbeat-test").build();

    let remote = RemoteActorSystem::builder()
        .with_tag("remote-1")
        .with_version(VERSION)
        .with_id(1)
        .with_actor_system(actor_system)
        .configure(|c| c)
        .build()
        .await;

    let health_check = Heartbeat::get_system_health(&remote).await;
    assert_eq!(health_check.status, HealthStatus::Healthy);
    assert_eq!(&health_check.node_version, VERSION);

    let actor = SlowActor
        .into_actor(Some("slow-actor"), remote.actor_system())
        .await
        .unwrap();
    let _ = actor.notify(Delay(Duration::from_secs(2)));

    let health_check = Heartbeat::get_system_health(&remote).await;
    assert_eq!(health_check.status, HealthStatus::Degraded);

    let _ = actor.notify_stop();
    let health_check = Heartbeat::get_system_health(&remote).await;
    assert_eq!(health_check.status, HealthStatus::Unhealthy);

    // De-register the actor from the health check
    Heartbeat::remove(actor.actor_id(), &remote);

    let health_check = Heartbeat::get_system_health(&remote).await;
    assert_eq!(health_check.status, HealthStatus::Healthy);
}

pub struct SlowActor;

#[async_trait]
impl Actor for SlowActor {
    async fn started(&mut self, ctx: &mut ActorContext) {
        Heartbeat::register(self.actor_ref(ctx), ctx.system().remote());
    }
}

struct Delay(Duration);

impl Message for Delay {
    type Result = ();
}

#[async_trait]
impl Handler<Delay> for SlowActor {
    async fn handle(&mut self, message: Delay, _ctx: &mut ActorContext) {
        tokio::time::sleep(message.0).await;
    }
}
