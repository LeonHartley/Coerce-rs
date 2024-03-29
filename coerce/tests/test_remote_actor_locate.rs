pub mod util;

#[macro_use]
extern crate serde;

#[macro_use]
extern crate async_trait;

#[macro_use]
extern crate coerce_macros;

use coerce::actor::scheduler::ActorType::Tracked;
use coerce::actor::system::ActorSystem;
use coerce::actor::IntoActorId;

use coerce::remote::system::RemoteActorSystem;

#[coerce_test]
pub async fn test_remote_actor_locate_node_locally() {
    util::create_trace_logger();

    let system = ActorSystem::new();
    let remote = RemoteActorSystem::builder()
        .with_actor_system(system.clone())
        .build()
        .await;

    let locate_before_creation = remote.locate_actor_node("leon".into_actor_id()).await;

    let _ = system
        .new_actor(
            "leon".to_string(),
            util::TestActor {
                status: None,
                counter: 0,
            },
            Tracked,
        )
        .await;

    let locate_after_creation = remote.locate_actor_node("leon".into_actor_id()).await;

    assert!(locate_before_creation.is_none());
    assert!(locate_after_creation.is_some());
}

#[tokio::test]
pub async fn test_remote_actor_locate_remotely() {
    util::create_trace_logger();

    let system_a = ActorSystem::new();
    let system_b = ActorSystem::new();

    let remote_a = RemoteActorSystem::builder()
        .with_actor_system(system_a.clone())
        .with_id(1)
        .with_tag("remote-a")
        .build()
        .await;

    let remote_b = RemoteActorSystem::builder()
        .with_actor_system(system_b.clone())
        .with_id(2)
        .with_tag("remote-b")
        .build()
        .await;

    remote_a
        .clone()
        .cluster_worker()
        .listen_addr("localhost:30101")
        .start()
        .await;

    remote_b
        .clone()
        .cluster_worker()
        .listen_addr("localhost:30102")
        .with_seed_addr("localhost:30101")
        .start()
        .await;

    let locate_before_creation_a = remote_a.locate_actor_node("leon".into_actor_id()).await;
    let locate_before_creation_b = remote_b.locate_actor_node("leon".into_actor_id()).await;

    assert_eq!(locate_before_creation_a, None);
    assert_eq!(locate_before_creation_b, None);

    let _ = system_a
        .new_actor(
            "leon",
            util::TestActor {
                status: None,
                counter: 0,
            },
            Tracked,
        )
        .await;

    let local_ref = remote_a
        .actor_ref::<util::TestActor>("leon".into_actor_id())
        .await
        .expect("unable to get local ref");

    let remote_ref = remote_b
        .actor_ref::<util::TestActor>("leon".into_actor_id())
        .await
        .expect("unable to get remote ref");

    assert_eq!(remote_ref.is_remote(), true);
    assert_eq!(local_ref.is_local(), true);
}
