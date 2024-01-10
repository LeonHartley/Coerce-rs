use coerce::actor::system::builder::ActorSystemBuilder;
use coerce::actor::system::ActorSystem;
use coerce::actor::Actor;
use coerce::remote::cluster::singleton::factory::SingletonFactory;
use coerce::remote::cluster::singleton::{singleton, SingletonBuilder};
use coerce::remote::system::RemoteActorSystem;
use std::time::Duration;
use tokio::time::sleep;
use tracing::Level;

mod util;

struct SingletonActor {}

impl Actor for SingletonActor {}

struct Factory {}

impl SingletonFactory for Factory {
    type Actor = SingletonActor;

    fn create(&self) -> Self::Actor {
        SingletonActor {}
    }
}

#[tokio::test]
pub async fn test_cluster_singleton_start() {
    util::create_logger(Some(Level::DEBUG));

    let system = ActorSystem::builder().system_name("node-1").build();
    let system2 = ActorSystem::builder().system_name("node-2").build();

    let remote = RemoteActorSystem::builder()
        .with_tag("remote-1")
        .with_id(1)
        .with_actor_system(system)
        .configure(singleton::<Factory>)
        .build()
        .await;

    let remote2 = RemoteActorSystem::builder()
        .with_tag("remote-2")
        .with_id(2)
        .with_actor_system(system2)
        .configure(singleton::<Factory>)
        .build()
        .await;

    remote
        .clone()
        .cluster_worker()
        .listen_addr("localhost:30101")
        .start()
        .await;

    remote2
        .clone()
        .cluster_worker()
        .listen_addr("localhost:30102")
        .with_seed_addr("localhost:30101")
        .start()
        .await;

    let singleton = SingletonBuilder::new(remote)
        .factory(Factory {})
        .build()
        .await;

    let singleton2 = SingletonBuilder::new(remote2)
        .factory(Factory {})
        .build()
        .await;

    sleep(Duration::from_secs(5)).await;
}
