use crate::actor::stream::{ChatMessage, ChatStream, ChatStreamFactory, Join};
use crate::websocket::start;

use coerce::actor::system::ActorSystem;
use coerce::persistent::journal::provider::inmemory::InMemoryStorageProvider;
use coerce::persistent::Persistence;
use coerce::remote::api::cluster::ClusterApi;
use coerce::remote::api::sharding::ShardingApi;
use coerce::remote::api::system::SystemApi;
use coerce::remote::api::RemoteHttpApi;
use coerce::remote::cluster::sharding::Sharding;
use coerce::remote::system::builder::RemoteActorSystemBuilder;
use coerce::remote::system::{NodeId, RemoteActorSystem};

use coerce_k8s::config::KubernetesDiscoveryConfig;
use coerce_k8s::KubernetesDiscovery;
use coerce_redis::journal::{RedisStorageConfig, RedisStorageProvider};
use std::net::SocketAddr;
use std::ptr::addr_of;
use std::str::FromStr;
use tokio::task::JoinHandle;

pub struct ShardedChatConfig {
    pub node_id: NodeId,
    pub remote_listen_addr: String,
    pub remote_seed_addr: Option<String>,
    pub websocket_listen_addr: String,
    pub cluster_api_listen_addr: String,
    pub persistence: ShardedChatPersistence,
}

pub enum ShardedChatPersistence {
    Redis { host: Option<String> },
    InMemory,
}

pub struct ShardedChat {
    system: RemoteActorSystem,
    sharding: Sharding<ChatStreamFactory>,
    listen_task: Option<JoinHandle<()>>,
    cluster_api_listen_task: Option<JoinHandle<()>>,
}

impl ShardedChat {
    pub async fn start(config: ShardedChatConfig) -> ShardedChat {
        let system = create_actor_system(&config).await;
        let sharding = Sharding::builder(system.clone())
            .with_entity_type("ChatStream")
            .build()
            .await;

        let listen_task = tokio::spawn(start(
            config.websocket_listen_addr,
            system.actor_system().clone(),
            sharding.clone(),
        ));

        let cluster_api = ClusterApi::new(system.clone());
        let system_api = SystemApi::new(system.clone());
        let sharding_api = ShardingApi::default()
            .attach(&sharding)
            .start(system.actor_system())
            .await;

        let cluster_api_listen_task = tokio::spawn(
            RemoteHttpApi::new(
                SocketAddr::from_str(&config.cluster_api_listen_addr).unwrap(),
                system.clone(),
            )
            .routes(&cluster_api)
            .routes(&sharding_api)
            .routes(&system_api)
            .start(),
        );

        ShardedChat {
            system,
            sharding,
            listen_task: Some(listen_task),
            cluster_api_listen_task: Some(cluster_api_listen_task),
        }
    }

    pub async fn stop(&mut self) {
        info!("shutting down");

        self.system.actor_system().shutdown().await;
        if let Some(listen_task) = self.listen_task.take() {
            info!("aborted TCP listen task");
            listen_task.abort();
        }

        if let Some(cluster_api_listen_task) = self.cluster_api_listen_task.take() {
            info!("aborted HTTP listen task");
            cluster_api_listen_task.abort();
        }

        info!("shutdown complete");
    }
}

async fn create_actor_system(config: &ShardedChatConfig) -> RemoteActorSystem {
    let system = ActorSystem::new().to_persistent(
        /*match &config.persistence {
        ShardedChatPersistence::Redis { host: Some(host) } => Persistence::from(
            RedisStorageProvider::connect(RedisStorageConfig {
                address: host.to_string(),
                key_prefix: "".to_string(),
                use_key_hashtags: false,
            })
            .await,
        ),
        _ => */
        Persistence::from(InMemoryStorageProvider::new()), /*,
                                                           }*/
    );

    let mut seed_addr = config.remote_seed_addr.clone();
    let is_running_in_k8s = std::env::var("KUBERNETES_SERVICE_HOST").is_ok();
    let (node_id, node_tag, external_addr) = if is_running_in_k8s {
        let pod_name = std::env::var("POD_NAME")
            .expect("POD_NAME environment variable not set, TODO: fallback to hostname?");
        let cluster_ip = std::env::var("CLUSTER_IP")
            .expect("CLUSTER_IP environment variable not set, TODO: fallback to hostname?");

        let discovered_targets =
            KubernetesDiscovery::discover(KubernetesDiscoveryConfig::default()).await;

        if let Some(peers) = discovered_targets {
            if let Some(first_peer) = peers.into_iter().next() {
                seed_addr = Some(first_peer);
            }
        }

        let pod_ordinal = pod_name.split("-").last();
        if let Some(Ok(pod_ordinal)) = pod_ordinal.map(|i| i.parse()) {
            let listen_addr_base = SocketAddr::from_str(&config.remote_listen_addr).unwrap();
            let port = listen_addr_base.port();

            let external_addr = format!("{}:{}", &cluster_ip, port);
            (pod_ordinal, pod_name, Some(external_addr))
        } else {
            (
                config.node_id,
                format!("chat-server-{}", config.node_id),
                None,
            )
        }
    } else {
        (
            config.node_id,
            format!("chat-server-{}", config.node_id),
            None,
        )
    };

    info!(
        "starting cluster node (node_id={}, node_tag={}, listen_addr={}, external_addr={:?})",
        node_id, &node_tag, &config.remote_listen_addr, &external_addr
    );

    let remote_system = RemoteActorSystemBuilder::new()
        .with_id(node_id)
        .with_tag(node_tag)
        .with_actor_system(system)
        .with_handlers(|handlers| {
            handlers
                .with_actor(ChatStreamFactory)
                .with_handler::<ChatStream, Join>("ChatStream.Join")
                .with_handler::<ChatStream, ChatMessage>("ChatStream.ChatMessage")
        })
        .build()
        .await;

    let mut cluster_worker = remote_system
        .clone()
        .cluster_worker()
        .listen_addr(&config.remote_listen_addr);

    if let Some(external_addr) = external_addr {
        cluster_worker = cluster_worker.external_addr(external_addr);
    } else {
        cluster_worker =
            cluster_worker.external_addr(&config.remote_listen_addr.replace("0.0.0.0", "127.0.0.1"))
    }

    // if is_running_in_k8s {
    //     if let Some(discovered_targets) = discovered_targets {
    //         if let Some(first_peer) = discovered_targets.into_iter().next() {
    //             cluster_worker = cluster_worker.with_seed_addr(first_peer)
    //         }
    //     }
    // } else {
    if let Some(seed_addr) = seed_addr {
        cluster_worker = cluster_worker.with_seed_addr(seed_addr.clone());
    }
    // }

    cluster_worker.start().await;
    remote_system
}
