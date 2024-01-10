use crate::actor::message::{Handler, Message};
use crate::actor::system::ActorSystem;
use crate::actor::{Actor, ActorFactory, IntoActor};
use crate::remote::actor::message::SetRemote;
use crate::remote::actor::{
    clients::RemoteClientRegistry, registry::RemoteRegistry, BoxedActorHandler,
    BoxedMessageHandler, RemoteHandler,
};
use crate::remote::handler::{RemoteActorHandler, RemoteActorMessageHandler};
use crate::remote::heartbeat::{Heartbeat, HeartbeatConfig};
use crate::remote::stream::mediator::StreamMediator;
use crate::remote::system::{AtomicNodeId, NodeId, RemoteActorSystem, RemoteSystemCore};

use rand::RngCore;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::actor::scheduler::ActorType;
use crate::remote::cluster::discovery::NodeDiscovery;

use crate::remote::cluster::node::NodeAttributes;
use crate::remote::config::{RemoteSystemConfig, RemoteSystemSecurity};

use crate::remote::net::security::ClientAuth;
use chrono::Utc;
use uuid::Uuid;

pub struct RemoteActorSystemBuilder {
    node_id: Option<NodeId>,
    node_tag: Option<String>,
    node_version: Option<String>,
    inner: Option<ActorSystem>,
    config_builders: Vec<ConfigBuilderFn>,
    mediator: Option<StreamMediator>,
    client_auth: Option<ClientAuth>,
    single_node_cluster: bool,
    node_attributes: HashMap<String, String>,
}

impl RemoteActorSystemBuilder {
    pub fn new() -> RemoteActorSystemBuilder {
        let mediator = StreamMediator::new();

        RemoteActorSystemBuilder {
            node_id: None,
            node_tag: None,
            node_version: None,
            inner: None,
            config_builders: vec![
                #[cfg(feature = "sharding")]
                Box::new(crate::sharding::sharding),
            ],
            mediator: Some(mediator),
            single_node_cluster: false,
            client_auth: None,
            node_attributes: Default::default(),
        }
    }

    pub fn with_tag(mut self, tag: impl Into<String>) -> Self {
        self.node_tag = Some(tag.into());

        self
    }

    pub fn with_id(mut self, id: NodeId) -> Self {
        self.node_id = Some(id);

        self
    }

    pub fn with_version(mut self, version: impl ToString) -> Self {
        self.node_version = Some(version.to_string());

        self
    }

    pub fn configure<F>(mut self, f: F) -> Self
    where
        F: 'static + (FnOnce(&mut RemoteSystemConfigBuilder) -> &mut RemoteSystemConfigBuilder),
    {
        self.config_builders.push(Box::new(f));

        self
    }

    pub fn with_handlers<F>(self, f: F) -> Self
    where
        F: 'static + (FnOnce(&mut RemoteSystemConfigBuilder) -> &mut RemoteSystemConfigBuilder),
    {
        self.configure(f)
    }

    pub fn with_actors<F>(self, f: F) -> Self
    where
        F: 'static + (FnOnce(&mut RemoteSystemConfigBuilder) -> &mut RemoteSystemConfigBuilder),
    {
        self.configure(f)
    }

    pub fn with_actor_system(mut self, sys: ActorSystem) -> Self {
        self.inner = Some(sys);

        self
    }

    pub fn single_node(mut self) -> Self {
        self.single_node_cluster = true;
        self
    }

    #[cfg(feature = "client-auth-jwt")]
    pub fn client_auth_jwt<S: Into<Vec<u8>>>(
        mut self,
        secret: S,
        token_ttl: Option<Duration>,
    ) -> Self {
        use crate::remote::net::security::jwt::Jwt;

        self.client_auth = Some(ClientAuth::Jwt(Jwt::from_secret(secret, token_ttl)));
        self
    }

    pub fn attribute<K: ToString, V: ToString>(mut self, key: K, value: V) -> Self {
        self.node_attributes
            .insert(key.to_string(), value.to_string());
        self
    }

    pub async fn build(self) -> RemoteActorSystem {
        // TODO: This needs cleaning up!

        let mut inner = match self.inner {
            Some(ctx) => ctx,
            None => ActorSystem::global_system(),
        };

        let mut config_builder = RemoteSystemConfigBuilder::new(inner.clone());

        self.config_builders.into_iter().for_each(|h| {
            h(&mut config_builder);
        });

        let node_id = self.node_id.unwrap_or_else(|| {
            let mut rand = rand::thread_rng();

            rand.next_u64()
        });

        let system_tag = self
            .node_tag
            .clone()
            .unwrap_or_else(|| inner.system_name().to_string());
        let config = config_builder.build(
            Some(system_tag.clone()),
            self.node_version,
            self.client_auth,
            self.node_attributes,
        );

        let handler_ref = Arc::new(parking_lot::Mutex::new(RemoteHandler::new()));
        let registry_ref = RemoteRegistry::new(&inner).await;
        let clients_ref = RemoteClientRegistry::new(&mut inner).await;
        let registry_ref_clone = registry_ref.clone();

        let discovery_ref = NodeDiscovery::default()
            .into_actor(Some("node-discovery"), &inner)
            .await
            .expect("unable to create NodeDiscovery actor");

        let heartbeat_ref = Heartbeat::start(&inner, config.heartbeat_config().clone()).await;

        let mediator_ref = if let Some(mediator) = self.mediator {
            trace!("mediator set");
            Some(
                inner
                    .new_actor("pubsub-mediator", mediator, ActorType::Tracked)
                    .await
                    .expect("unable to start mediator actor"),
            )
        } else {
            trace!("no mediator");
            None
        };

        let mut core = RemoteSystemCore {
            node_id,
            inner,
            handler_ref,
            registry_ref,
            clients_ref,
            mediator_ref,
            discovery_ref,
            heartbeat_ref,
            started_at: Utc::now(),
            config,
            current_leader: Arc::new(AtomicNodeId::new(if self.single_node_cluster {
                node_id as i64
            } else {
                -1
            })),
        };

        let inner = Arc::new(core.clone());
        let system = RemoteActorSystem { inner };

        core.inner = core.inner.to_remote(system.clone());

        let system = RemoteActorSystem {
            inner: Arc::new(core),
        };

        registry_ref_clone
            .send(SetRemote(system.clone()))
            .await
            .expect("no system set");

        system
            .heartbeat()
            .send(SetRemote(system.clone()))
            .await
            .expect("no system set");

        system
            .node_discovery()
            .send(SetRemote(system.clone()))
            .await
            .expect("no system set");

        if let Some(stream_mediator) = system.stream_mediator() {
            stream_mediator
                .send(SetRemote(system.clone()))
                .await
                .expect("no system set");
        }

        system
            .actor_system()
            .scheduler()
            .send(SetRemote(system.clone()))
            .await
            .expect("no system set");

        system
    }
}

pub(crate) type ConfigBuilderFn =
    Box<dyn (FnOnce(&mut RemoteSystemConfigBuilder) -> &mut RemoteSystemConfigBuilder)>;

pub struct RemoteSystemConfigBuilder {
    system: ActorSystem,
    heartbeat: Option<HeartbeatConfig>,
    actors: HashMap<String, BoxedActorHandler>,
    handlers: HashMap<String, BoxedMessageHandler>,
}

impl RemoteSystemConfigBuilder {
    pub fn new(system: ActorSystem) -> RemoteSystemConfigBuilder {
        RemoteSystemConfigBuilder {
            actors: HashMap::new(),
            handlers: HashMap::new(),
            system,
            heartbeat: None,
        }
    }

    pub fn with_handler<A: Actor, M: Message>(&mut self, identifier: impl ToString) -> &mut Self
    where
        A: Handler<M>,
    {
        let handler = RemoteActorMessageHandler::<A, M>::new(self.system.clone());
        self.handlers.insert(identifier.to_string(), handler);

        self
    }

    pub fn with_actor<F: ActorFactory>(&mut self, factory: F) -> &mut Self
    where
        F: 'static + ActorFactory + Send + Sync,
    {
        let handler = Box::new(RemoteActorHandler::<F::Actor, F>::new(factory));

        self.actors
            .insert(String::from(F::Actor::type_name()), handler);
        self
    }

    pub fn heartbeat(&mut self, heartbeat_config: HeartbeatConfig) -> &mut Self {
        self.heartbeat = Some(heartbeat_config);
        self
    }

    pub fn build(
        self,
        tag: Option<String>,
        version: Option<String>,
        client_auth: Option<ClientAuth>,
        attributes: HashMap<String, String>,
    ) -> Arc<RemoteSystemConfig> {
        let mut handler_types = HashMap::new();
        let mut actor_types = HashMap::new();

        for (k, v) in &self.handlers {
            let _ = handler_types.insert(v.id(), k.clone());
        }

        for (k, v) in &self.actors {
            let _ = actor_types.insert(v.id(), k.clone());
        }

        let node_tag = tag.map_or_else(|| format!("cluster-node-{}", Uuid::new_v4()), |t| t);
        let node_version = version.unwrap_or_else(|| "0.0.0".to_string());
        let attributes = attributes
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect::<NodeAttributes>()
            .into();

        Arc::new(RemoteSystemConfig::new(
            node_tag,
            node_version,
            actor_types,
            handler_types,
            self.handlers,
            self.actors,
            self.heartbeat.unwrap_or_default(),
            attributes,
            RemoteSystemSecurity::new(client_auth.unwrap_or_default()),
        ))
    }
}
