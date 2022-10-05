use crate::remote::cluster::node::RemoteNodeStore;

use crate::actor::message::Message;
use crate::actor::system::ActorSystem;
use crate::actor::{scheduler::ActorType, Actor, ActorId, ActorRefErr, LocalActorRef};
use crate::remote::handler::{
    ActorHandler, ActorMessageHandler, RemoteActorMarker, RemoteActorMessageMarker,
};
use crate::remote::net::client::RemoteClient;
use crate::remote::system::{NodeId, RemoteActorSystem};
use std::any::TypeId;
use std::collections::HashMap;

use crate::actor::context::ActorContext;
use crate::actor::scheduler::ActorType::Anonymous;
use crate::remote::heartbeat::HeartbeatConfig;
use crate::remote::stream::pubsub::Subscription;
use uuid::Uuid;

pub mod handler;
pub mod message;

pub struct RemoteClientRegistry {
    node_addr_registry: HashMap<String, LocalActorRef<RemoteClient>>,
    node_id_registry: HashMap<NodeId, LocalActorRef<RemoteClient>>,
}

pub struct RemoteRegistry {
    nodes: RemoteNodeStore,
    actors: HashMap<ActorId, NodeId>,
    system: Option<RemoteActorSystem>,
    system_event_subscription: Option<Subscription>,
}

#[derive(Serialize, Deserialize, Clone, Default)]
pub struct SystemCapabilities {
    pub actors: Vec<String>,
    pub messages: Vec<String>,
}

pub(crate) type BoxedActorHandler = Box<dyn ActorHandler + Send + Sync>;

pub(crate) type BoxedMessageHandler = Box<dyn ActorMessageHandler + Send + Sync>;

pub struct RemoteSystemConfig {
    node_tag: String,
    actor_types: HashMap<TypeId, String>,
    handler_types: HashMap<TypeId, String>,
    message_handlers: HashMap<String, BoxedMessageHandler>,
    actor_handlers: HashMap<String, BoxedActorHandler>,
    heartbeat_config: HeartbeatConfig,
    server_auth_token: Option<String>,
}

impl RemoteSystemConfig {
    pub fn new(
        node_tag: String,
        actor_types: HashMap<TypeId, String>,
        handler_types: HashMap<TypeId, String>,
        message_handlers: HashMap<String, BoxedMessageHandler>,
        actor_handlers: HashMap<String, BoxedActorHandler>,
        heartbeat_config: HeartbeatConfig,
        server_auth_token: Option<String>,
    ) -> RemoteSystemConfig {
        RemoteSystemConfig {
            node_tag,
            actor_types,
            handler_types,
            message_handlers,
            actor_handlers,
            heartbeat_config,
            server_auth_token,
        }
    }

    pub fn node_tag(&self) -> &str {
        &self.node_tag
    }

    pub fn handler_name<A: Actor, M: Message>(&self) -> Option<String> {
        let marker = RemoteActorMessageMarker::<A, M>::new();
        self.handler_types.get(&marker.id()).cloned()
    }

    pub fn actor_name<A: Actor>(&self, marker: RemoteActorMarker<A>) -> Option<String>
    where
        A: 'static + Send + Sync,
    {
        self.actor_types.get(&marker.id()).map(|name| name.clone())
    }

    pub fn message_handler(&self, key: &str) -> Option<BoxedMessageHandler> {
        self.message_handlers
            .get(key)
            .map(|handler| handler.new_boxed())
    }

    pub fn actor_handler(&self, key: &str) -> Option<BoxedActorHandler> {
        self.actor_handlers
            .get(key)
            .map(|handler| handler.new_boxed())
    }

    pub fn heartbeat_config(&self) -> &HeartbeatConfig {
        &self.heartbeat_config
    }

    pub fn get_capabilities(&self) -> SystemCapabilities {
        let mut actors: Vec<String> = self.actor_types.values().map(|a| a.clone()).collect();
        actors.sort_by(|a, b| a.to_lowercase().cmp(&b.to_lowercase()));

        let mut messages: Vec<String> = self.handler_types.values().map(|a| a.clone()).collect();
        messages.sort_by(|a, b| a.to_lowercase().cmp(&b.to_lowercase()));

        SystemCapabilities { actors, messages }
    }
}

pub struct RemoteHandler {
    requests: HashMap<Uuid, RemoteRequest>,
}

impl RemoteHandler {
    pub fn push_request(&mut self, message_id: Uuid, request: RemoteRequest) {
        self.requests.insert(message_id, request);
    }

    pub fn pop_request(&mut self, message_id: Uuid) -> Option<RemoteRequest> {
        self.requests.remove(&message_id)
    }

    pub fn inflight_request_count(&self) -> usize {
        self.requests.len()
    }
}

pub struct RemoteRequest {
    pub res_tx: tokio::sync::oneshot::Sender<RemoteResponse>,
}

#[derive(Debug)]
pub enum RemoteResponse {
    Ok(Vec<u8>),
    Err(ActorRefErr),
}

impl RemoteResponse {
    pub fn is_ok(&self) -> bool {
        match self {
            &RemoteResponse::Ok(..) => true,
            _ => false,
        }
    }

    pub fn is_err(&self) -> bool {
        match self {
            &RemoteResponse::Err(..) => true,
            _ => false,
        }
    }

    pub fn into_result(self) -> Result<Vec<u8>, ActorRefErr> {
        match self {
            RemoteResponse::Ok(buff) => Ok(buff),
            RemoteResponse::Err(buff) => Err(buff),
        }
    }
}

impl Actor for RemoteRegistry {}

#[async_trait]
impl Actor for RemoteClientRegistry {
    async fn stopped(&mut self, _ctx: &mut ActorContext) {
        for client in &self.node_id_registry {
            let _ = client.1.stop().await;
        }
    }
}

impl RemoteClientRegistry {
    pub async fn new(ctx: &ActorSystem, system_tag: &str) -> LocalActorRef<RemoteClientRegistry> {
        ctx.new_actor(
            format!("RemoteClientRegistry-{}", system_tag),
            RemoteClientRegistry {
                node_addr_registry: HashMap::new(),
                node_id_registry: HashMap::new(),
            },
            ActorType::Tracked,
        )
        .await
        .expect("RemoteClientRegistry")
    }
}

impl RemoteRegistry {
    pub async fn new(ctx: &ActorSystem, system_tag: &str) -> LocalActorRef<RemoteRegistry> {
        ctx.new_actor(
            format!("RemoteRegistry-{}", &system_tag),
            RemoteRegistry {
                actors: HashMap::new(),
                nodes: RemoteNodeStore::new(vec![]),
                system: None,
                system_event_subscription: None,
            },
            ActorType::Tracked,
        )
        .await
        .expect("RemoteRegistry")
    }
}

impl RemoteHandler {
    pub fn new() -> RemoteHandler {
        RemoteHandler {
            requests: HashMap::new(),
        }
    }
}
