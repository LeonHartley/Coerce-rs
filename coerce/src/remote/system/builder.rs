use crate::actor::message::{Handler, Message};
use crate::actor::system::ActorSystem;
use crate::actor::{Actor, ActorFactory};
use crate::remote::actor::message::SetRemote;
use crate::remote::actor::{
    BoxedActorHandler, BoxedMessageHandler, RemoteClientRegistry, RemoteHandler, RemoteRegistry,
    RemoteSystemConfig,
};
use crate::remote::handler::{RemoteActorHandler, RemoteActorMessageHandler};
use crate::remote::heartbeat::{Heartbeat, HeartbeatConfig};
use crate::remote::raft::RaftSystem;
use crate::remote::stream::mediator::StreamMediator;
use crate::remote::stream::system::SystemTopic;
use crate::remote::system::{AtomicNodeId, NodeId, RemoteActorSystem, RemoteSystemCore};

use futures::TryFutureExt;
use rand::RngCore;

use std::collections::HashMap;
use std::sync::Arc;

use crate::actor::scheduler::ActorType;
use crate::remote::cluster::sharding::coordinator::allocation::AllocateShard;
use crate::remote::cluster::sharding::coordinator::ShardCoordinator;
use crate::remote::cluster::sharding::host::request::RemoteEntityRequest;
use crate::remote::cluster::sharding::shard::Shard;
use crate::remote::cluster::sharding::sharding;
use chrono::Utc;
use uuid::Uuid;

pub struct RemoteActorSystemBuilder {
    node_id: Option<NodeId>,
    node_tag: Option<String>,
    inner: Option<ActorSystem>,
    handlers: Vec<HandlerFn>,
    mediator: Option<StreamMediator>,
    single_node_cluster: bool,
}

impl RemoteActorSystemBuilder {
    pub fn new() -> RemoteActorSystemBuilder {
        let mut mediator = StreamMediator::new();
        mediator.add_topic::<SystemTopic>();

        RemoteActorSystemBuilder {
            node_id: None,
            node_tag: None,
            inner: None,
            handlers: vec![Box::new(sharding)],
            mediator: Some(mediator),
            single_node_cluster: false,
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

    pub fn with_actors<F>(mut self, f: F) -> Self
    where
        F: 'static + (FnOnce(&mut RemoteActorHandlerBuilder) -> &mut RemoteActorHandlerBuilder),
    {
        self.handlers.push(Box::new(f));

        self
    }

    pub fn with_handlers<F>(mut self, f: F) -> Self
    where
        F: 'static + (FnOnce(&mut RemoteActorHandlerBuilder) -> &mut RemoteActorHandlerBuilder),
    {
        self.handlers.push(Box::new(f));

        self
    }

    pub fn with_actor_system(mut self, sys: ActorSystem) -> Self {
        self.inner = Some(sys);

        self
    }

    pub fn with_distributed_streams<F>(mut self, f: F) -> Self
    where
        F: 'static + (FnOnce(&mut StreamMediator) -> &mut StreamMediator),
    {
        let mediator = if let Some(mediator) = &mut self.mediator {
            mediator
        } else {
            let mediator = StreamMediator::new();
            self.mediator = Some(mediator);
            self.mediator.as_mut().unwrap()
        };

        f(mediator);
        self
    }

    pub fn single_node(mut self) -> Self {
        self.single_node_cluster = true;
        self
    }

    pub async fn build(self) -> RemoteActorSystem {
        let span = tracing::trace_span!("RemoteActorSystemBuilder::build");
        let _enter = span.enter();

        let mut inner = match self.inner {
            Some(ctx) => ctx,
            None => ActorSystem::current_system(),
        };

        let mut handlers = RemoteActorHandlerBuilder::new(inner.clone());

        self.handlers.into_iter().for_each(|h| {
            h(&mut handlers);
        });

        let node_id = self.node_id.unwrap_or_else(|| {
            let mut rand = rand::thread_rng();

            rand.next_u64()
        });

        let system_tag = self.node_tag.clone().unwrap_or_else(|| node_id.to_string());

        let config = handlers.build(self.node_tag);
        let handler_ref = Arc::new(parking_lot::Mutex::new(RemoteHandler::new()));
        let registry_ref = RemoteRegistry::new(&inner, &system_tag).await;

        let clients_ref = RemoteClientRegistry::new(&mut inner, &system_tag).await;
        let registry_ref_clone = registry_ref.clone();

        let heartbeat_ref = Heartbeat::start(
            &system_tag,
            HeartbeatConfig {
                ..HeartbeatConfig::default()
            },
            &inner,
        )
        .await;

        let heartbeat_ref_clone = heartbeat_ref.clone();
        let heartbeat_ref = Some(heartbeat_ref);

        let mediator_ref = if let Some(mediator) = self.mediator {
            trace!("mediator set");
            Some(
                inner
                    .new_actor(
                        format!("PubSubMediator-{}", &system_tag),
                        mediator,
                        ActorType::Anonymous,
                    )
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
            heartbeat_ref,
            raft: None,
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

        core.raft = None;
        core.inner.set_remote(system.clone());

        let system = RemoteActorSystem {
            inner: Arc::new(core),
        };

        registry_ref_clone
            .send(SetRemote(system.clone()))
            .await
            .expect("no system set");

        heartbeat_ref_clone
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

pub(crate) type HandlerFn =
    Box<dyn (FnOnce(&mut RemoteActorHandlerBuilder) -> &mut RemoteActorHandlerBuilder)>;

pub struct RemoteActorHandlerBuilder {
    system: ActorSystem,
    actors: HashMap<String, BoxedActorHandler>,
    handlers: HashMap<String, BoxedMessageHandler>,
}

impl RemoteActorHandlerBuilder {
    pub fn new(system: ActorSystem) -> RemoteActorHandlerBuilder {
        RemoteActorHandlerBuilder {
            actors: HashMap::new(),
            handlers: HashMap::new(),
            system,
        }
    }

    pub fn with_handler<A: Actor, M: Message>(&mut self, identifier: &'static str) -> &mut Self
    where
        A: Handler<M>,
    {
        let handler = RemoteActorMessageHandler::<A, M>::new(self.system.clone());
        self.handlers.insert(String::from(identifier), handler);

        self
    }

    pub fn with_actor<F: ActorFactory>(&mut self, factory: F) -> &mut Self
    where
        F: 'static + ActorFactory + Send + Sync,
    {
        let handler = Box::new(RemoteActorHandler::<F::Actor, F>::new(
            self.system.clone(),
            factory,
        ));

        self.actors
            .insert(String::from(F::Actor::type_name()), handler);
        self
    }

    pub fn build(self, tag: Option<String>) -> Arc<RemoteSystemConfig> {
        let mut handler_types = HashMap::new();
        let mut actor_types = HashMap::new();

        for (k, v) in &self.handlers {
            let _ = handler_types.insert(v.id(), k.clone());
        }

        for (k, v) in &self.actors {
            let _ = actor_types.insert(v.id(), k.clone());
        }

        let node_tag = tag.map_or_else(|| format!("cluster-node-{}", Uuid::new_v4()), |t| t);
        Arc::new(RemoteSystemConfig::new(
            node_tag,
            actor_types,
            handler_types,
            self.handlers,
            self.actors,
        ))
    }
}
