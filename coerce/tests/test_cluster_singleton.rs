use std::time::Duration;
use tokio::time::sleep;
use tracing::Level;
use coerce::actor::Actor;
use coerce::actor::system::ActorSystem;
use coerce::remote::cluster::singleton::SingletonBuilder;
use coerce::remote::system::RemoteActorSystem;

mod util;

struct SingletonActor {

}

impl Actor for SingletonActor {

}

#[tokio::test]
pub async fn test_cluster_singleton_start() {
    util::create_logger(Some(Level::DEBUG));

    let system = ActorSystem::new();
    let system2 = ActorSystem::new();
    let remote = RemoteActorSystem::builder()
        .with_tag("remote-1")
        .with_id(1)
        .with_actor_system(system)
        .build()
        .await;

    let remote2 = RemoteActorSystem::builder()
        .with_tag("remote-2")
        .with_id(2)
        .with_actor_system(system2)
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
        .factory(|| SingletonActor {})
        .build().await;

    let singleton2 = SingletonBuilder::new(remote2)
        .factory(|| SingletonActor {})
        .build().await;

    sleep(Duration::from_secs(5)).await;
}
