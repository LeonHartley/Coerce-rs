use crate::actor::pubsub::ChatStreamTopic;
use crate::actor::stream::{ChatMessage, ChatStream, ChatStreamFactory, Join};
use crate::websocket::start;

use coerce::actor::system::ActorSystem;
use coerce::persistent::journal::provider::inmemory::InMemoryStorageProvider;
use coerce::remote::api::cluster::ClusterApi;
use coerce::remote::api::sharding::ShardingApi;
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
        let sharding = Sharding::start("ChatStream".to_string(), system.clone()).await;
        let listen_task = tokio::spawn(start(
            config.websocket_listen_addr,
            system.actor_system().clone(),
            sharding.clone(),
        ));

        let cluster_api = ClusterApi::new(system.clone());
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
        self.system.actor_system().shutdown().await;
        if let Some(listen_task) = self.listen_task.take() {
            listen_task.abort();
        }

        if let Some(cluster_api_listen_task) = self.cluster_api_listen_task.take() {
            cluster_api_listen_task.abort();
        }
    }
}

async fn create_actor_system(config: &ShardedChatConfig) -> RemoteActorSystem {
    let redis = RedisStorageProvider::connect(RedisStorageConfig {
        address: "127.0.0.1:6379".to_string(),
        key_prefix: "".to_string(),
        use_key_hashtags: false,
    })
    .await;

    let system = ActorSystem::new_persistent(redis);
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
