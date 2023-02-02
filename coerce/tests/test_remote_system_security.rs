#[macro_use]
extern crate serde;

#[macro_use]
extern crate tracing;

#[macro_use]
extern crate async_trait;

use coerce::actor::system::ActorSystem;
use coerce::remote::net::security::jwt::Jwt;
use coerce::remote::system::RemoteActorSystem;
use std::time::Duration;

mod util;

#[tokio::test]
pub async fn test_jwt_validation() {
    let secret = b"secret";
    let token_ttl = None;

    let jwt = Jwt::from_secret(secret.to_vec(), token_ttl);
    let token = jwt.generate_token().unwrap();
    let token_validated = jwt.validate_token(&token);
    assert!(!token.is_empty());
    assert!(token_validated);
}

#[tokio::test]
pub async fn test_jwt_validation_with_expiry() {
    let secret = b"secret";
    let token_ttl = Some(Duration::from_millis(100));
    let jwt = Jwt::from_secret(secret.to_vec(), token_ttl);
    let token = jwt.generate_token().unwrap();
    let token_validated = jwt.validate_token(&token);
    assert!(!token.is_empty());
    assert!(token_validated);

    tokio::time::sleep(Duration::from_millis(200)).await;
    let token_validated = jwt.validate_token(&token);
    assert!(!token_validated);
}

#[tokio::test]
pub async fn test_remote_cluster_inconsistent_secrets() {
    util::create_trace_logger();

    let secrets = vec!["secret-1", "secret-2", "secret-3"];

    let nodes = create_cluster_nodes("3102", secrets).await;

    let nodes_1 = nodes[0].get_nodes().await;
    let nodes_2 = nodes[1].get_nodes().await;
    let nodes_3 = nodes[2].get_nodes().await;

    assert_eq!(nodes_1.len(), 1);
    assert_eq!(nodes_2.len(), 1);
    assert_eq!(nodes_3.len(), 1);
}

#[tokio::test]
pub async fn test_remote_cluster_consistent_secrets() {
    util::create_trace_logger();

    let secrets = vec!["secret-1", "secret-1", "secret-1"];

    let nodes = create_cluster_nodes("3101", secrets).await;

    let nodes_1 = nodes[0].get_nodes().await;
    let nodes_2 = nodes[1].get_nodes().await;
    let nodes_3 = nodes[2].get_nodes().await;

    assert_eq!(nodes_1.len(), 3);
    assert_eq!(nodes_2.len(), 3);
    assert_eq!(nodes_3.len(), 3);
}

async fn create_cluster_nodes(
    port_prefix: &'static str,
    secrets: Vec<&'static str>,
) -> Vec<RemoteActorSystem> {
    let system = ActorSystem::new();
    let remote = RemoteActorSystem::builder()
        .with_tag("remote-1")
        .with_id(1)
        .with_actor_system(system)
        .client_auth_jwt(secrets[0], None)
        .build()
        .await;

    let remote_2 = RemoteActorSystem::builder()
        .with_tag("remote-2")
        .with_id(2)
        .with_actor_system(ActorSystem::new())
        .client_auth_jwt(secrets[1], None)
        .build()
        .await;

    let remote_3 = RemoteActorSystem::builder()
        .with_tag("remote-3")
        .with_id(3)
        .with_actor_system(ActorSystem::new())
        .client_auth_jwt(secrets[2], None)
        .build()
        .await;

    remote
        .clone()
        .cluster_worker()
        .listen_addr(format!("localhost:{}1", port_prefix))
        .start()
        .await;

    remote_2
        .clone()
        .cluster_worker()
        .listen_addr(format!("localhost:{}2", port_prefix))
        .with_seed_addr(format!("localhost:{}1", port_prefix))
        .start()
        .await;

    remote_3
        .clone()
        .cluster_worker()
        .listen_addr(format!("localhost:{}3", port_prefix))
        .with_seed_addr(format!("localhost:{}2", port_prefix))
        .start()
        .await;

    vec![remote, remote_2, remote_3]
}
