use crate::actor::message::Message;
use crate::actor::system::ActorSystem;
use crate::actor::{scheduler::ActorType, Actor, ActorId, ActorRefErr, LocalActorRef};
use crate::remote::cluster::node::{NodeAttributes, NodeAttributesRef, RemoteNodeStore};
use crate::remote::handler::{
    ActorHandler, ActorMessageHandler, RemoteActorMarker, RemoteActorMessageMarker,
};
use crate::remote::heartbeat::HeartbeatConfig;
use crate::remote::net::client::RemoteClient;
use crate::remote::stream::pubsub::Subscription;
use crate::remote::system::{NodeId, RemoteActorSystem};
use std::any::TypeId;
use std::collections::HashMap;
use uuid::Uuid;

pub mod clients;
pub mod message;
pub mod registry;

#[derive(Serialize, Deserialize, Clone, Default)]
pub struct SystemCapabilities {
    pub actors: Vec<String>,
    pub messages: Vec<String>,
}

pub(crate) type BoxedActorHandler = Box<dyn ActorHandler + Send + Sync>;

pub(crate) type BoxedMessageHandler = Box<dyn ActorMessageHandler + Send + Sync>;

pub struct RemoteSystemConfig {
    node_tag: String,
    node_version: String,
    actor_types: HashMap<TypeId, String>,
    handler_types: HashMap<TypeId, String>,
    message_handlers: HashMap<String, BoxedMessageHandler>,
    actor_handlers: HashMap<String, BoxedActorHandler>,
    heartbeat_config: HeartbeatConfig,
    server_auth_token: Option<String>,
    node_attributes: NodeAttributesRef,
}

impl RemoteSystemConfig {
    pub fn new(
        node_tag: String,
        node_version: String,
        actor_types: HashMap<TypeId, String>,
        handler_types: HashMap<TypeId, String>,
        message_handlers: HashMap<String, BoxedMessageHandler>,
        actor_handlers: HashMap<String, BoxedActorHandler>,
        heartbeat_config: HeartbeatConfig,
        server_auth_token: Option<String>,
        node_attributes: NodeAttributesRef,
    ) -> RemoteSystemConfig {
        RemoteSystemConfig {
            node_tag,
            node_version,
            actor_types,
            handler_types,
            message_handlers,
            actor_handlers,
            heartbeat_config,
            server_auth_token,
            node_attributes,
        }
    }

    pub fn node_tag(&self) -> &str {
        &self.node_tag
    }

    pub fn node_version(&self) -> &str {
        &self.node_version
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

    pub fn get_attributes(&self) -> &NodeAttributesRef {
        &self.node_attributes
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

impl RemoteHandler {
    pub fn new() -> RemoteHandler {
        RemoteHandler {
            requests: HashMap::new(),
        }
    }
}
