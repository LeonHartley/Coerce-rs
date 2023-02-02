use crate::actor::message::Message;
use crate::actor::Actor;
use crate::remote::actor::{BoxedActorHandler, BoxedMessageHandler};
use crate::remote::cluster::node::NodeAttributesRef;
use crate::remote::handler::{RemoteActorMarker, RemoteActorMessageMarker};
use crate::remote::heartbeat::HeartbeatConfig;
use crate::remote::net::security::ClientAuth;
use std::any::TypeId;
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Clone, Default)]
pub struct SystemCapabilities {
    pub actors: Vec<String>,
    pub messages: Vec<String>,
}

pub struct RemoteSystemConfig {
    node_tag: String,
    node_version: String,
    actor_types: HashMap<TypeId, String>,
    handler_types: HashMap<TypeId, String>,
    message_handlers: HashMap<String, BoxedMessageHandler>,
    actor_handlers: HashMap<String, BoxedActorHandler>,
    heartbeat_config: HeartbeatConfig,
    node_attributes: NodeAttributesRef,
    security: RemoteSystemSecurity,
}

#[derive(Default)]
pub struct RemoteSystemSecurity {
    client_auth: ClientAuth,
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
        node_attributes: NodeAttributesRef,
        security: RemoteSystemSecurity,
    ) -> RemoteSystemConfig {
        RemoteSystemConfig {
            node_tag,
            node_version,
            actor_types,
            handler_types,
            message_handlers,
            actor_handlers,
            heartbeat_config,
            node_attributes,
            security,
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

    pub fn security(&self) -> &RemoteSystemSecurity {
        &self.security
    }
}

impl RemoteSystemSecurity {
    pub fn new(client_auth: ClientAuth) -> Self {
        Self { client_auth }
    }

    pub fn client_authentication(&self) -> &ClientAuth {
        &self.client_auth
    }
}
