use crate::actor::pubsub::ChatStreamTopic;
use crate::actor::stream::{ChatMessage, ChatStream, ChatStreamFactory, Join};
use crate::websocket::start;

use coerce::actor::system::ActorSystem;
use coerce::persistent::journal::provider::inmemory::InMemoryStorageProvider;
use coerce::persistent::{ConfigurePersistence, Persistence};
use coerce::remote::api::cluster::ClusterApi;
use coerce::remote::api::sharding::ShardingApi;
use coerce::remote::api::system::SystemApi;
use coerce::remote::api::RemoteHttpApi;
use coerce::remote::cluster::sharding::Sharding;
use coerce::remote::system::builder::RemoteActorSystemBuilder;
use coerce::remote::system::{NodeId, RemoteActorSystem};

use coerce_redis::journal::{RedisStorageConfig, RedisStorageProvider};
use std::net::SocketAddr;
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
    let system = ActorSystem::new().add_persistence(match &config.persistence {
        ShardedChatPersistence::Redis { host: Some(host) } => Persistence::from(
            RedisStorageProvider::connect(RedisStorageConfig {
                address: host.to_string(),
                key_prefix: "".to_string(),
                use_key_hashtags: false,
            })
            .await,
        ),
        _ => Persistence::from(InMemoryStorageProvider::new()),
    });

    let remote_system = RemoteActorSystemBuilder::new()
        .with_id(config.node_id)
        .with_tag(format!("chat-server-{}", &config.node_id))
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
        .listen_addr(config.remote_listen_addr.clone());

    if let Some(seed_addr) = &config.remote_seed_addr {
        cluster_worker = cluster_worker.with_seed_addr(seed_addr.clone());
    }

    cluster_worker.start().await;
    remote_system
}
