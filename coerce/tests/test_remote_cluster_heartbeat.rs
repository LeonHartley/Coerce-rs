pub mod util;

#[macro_use]
extern crate coerce_macros;

#[macro_use]
extern crate async_trait;

#[macro_use]
extern crate serde;

use coerce::actor::system::ActorSystem;
use coerce::remote::cluster::node::NodeStatus::{Healthy, Terminated};
use coerce::remote::heartbeat::HeartbeatConfig;
use coerce::remote::system::builder::RemoteSystemConfigBuilder;
use coerce::remote::system::RemoteActorSystem;
use std::time::Duration;
use tokio::time;

#[coerce_test]
pub async fn test_remote_cluster_heartbeat() {
    util::create_trace_logger();
    fn configure_sys(c: &mut RemoteSystemConfigBuilder) -> &mut RemoteSystemConfigBuilder {
        c.heartbeat(HeartbeatConfig {
            interval: Duration::from_millis(250),
            ping_timeout: Duration::from_millis(10),
            unhealthy_node_heartbeat_timeout: Duration::from_millis(750),
            terminated_node_heartbeat_timeout: Duration::from_millis(1000),
            ..Default::default()
        })
    }

    let remote = RemoteActorSystem::builder()
        .with_tag("remote-1")
        .with_id(1)
        .with_actor_system(ActorSystem::new())
        .configure(configure_sys)
        .build()
        .await;

    let remote_2 = RemoteActorSystem::builder()
        .with_tag("remote-2")
        .with_id(2)
        .with_actor_system(ActorSystem::new())
        .configure(configure_sys)
        .build()
        .await;

    let server = remote
        .clone()
        .cluster_worker()
        .listen_addr("localhost:30101")
        .start()
        .await;

    let _server_2 = remote_2
        .clone()
        .cluster_worker()
        .listen_addr("localhost:30102")
        .with_seed_addr("localhost:30101")
        .start()
        .await;

    // ensure both nodes have run a heartbeat atleast once.
    // TODO: We need the ability to hook into an on-cluster-joined event/future
    tokio::time::sleep(Duration::from_millis(750)).await;

    let nodes_a = remote.get_nodes().await;
    let nodes_b = remote_2.get_nodes().await;
    let node_a_1 = nodes_a.iter().find(|n| n.id == 1).cloned().unwrap();
    let node_a_2 = nodes_a.iter().find(|n| n.id == 2).cloned().unwrap();
    let node_b_1 = nodes_b.iter().find(|n| n.id == 1).cloned().unwrap();
    let node_b_2 = nodes_b.iter().find(|n| n.id == 2).cloned().unwrap();

    let leader_1_id = remote.current_leader();
    let leader_2_id = remote_2.current_leader();

    assert_eq!(leader_1_id, Some(remote.node_id()));
    assert_eq!(leader_2_id, Some(remote.node_id()));

    assert_eq!(node_a_1.status, Healthy);
    assert_eq!(node_a_2.status, Healthy);
    assert_eq!(node_b_1.status, Healthy);
    assert_eq!(node_b_2.status, Healthy);

    {
        let server = server;
        let remote = remote;
        server.stop();
        remote.actor_system().shutdown().await;
    }

    time::sleep(Duration::from_secs(1)).await;

    let nodes_b = remote_2.get_nodes().await;
    let node_1 = nodes_b.iter().find(|n| n.id == 1).cloned().unwrap();
    let node_2 = nodes_b.iter().find(|n| n.id == 2).cloned().unwrap();

    let leader_id = remote_2.current_leader();

    assert_eq!(leader_id, Some(remote_2.node_id()));
    assert_eq!(node_1.status, Terminated);
    assert_eq!(node_2.status, Healthy);
}
