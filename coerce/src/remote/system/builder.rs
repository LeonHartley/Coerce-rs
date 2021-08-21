use crate::actor::message::{Handler, Message};
use crate::actor::scheduler::ActorType::{Anonymous, Tracked};
use crate::actor::system::ActorSystem;
use crate::actor::{Actor, ActorFactory};
use crate::remote::actor::message::SetRemote;
use crate::remote::actor::{
    BoxedActorHandler, BoxedMessageHandler, RemoteClientRegistry, RemoteHandler,
    RemoteHandlerTypes, RemoteRegistry,
};
use crate::remote::handler::{RemoteActorHandler, RemoteActorMessageHandler};
use crate::remote::stream::mediator::StreamMediator;
use crate::remote::stream::system::SystemTopic;
use crate::remote::system::{RemoteActorSystem, RemoteSystemCore};
use futures::TryFutureExt;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::any::TypeId;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

pub struct RemoteActorSystemBuilder {
    node_id: Option<Uuid>,
    node_tag: Option<String>,
    inner: Option<ActorSystem>,
    handlers: Vec<HandlerFn>,
    mediator: Option<StreamMediator>,
}

impl RemoteActorSystemBuilder {
    pub fn new() -> RemoteActorSystemBuilder {
        let mut mediator = StreamMediator::new();
        mediator.add_topic::<SystemTopic>();

        RemoteActorSystemBuilder {
            node_id: None,
            node_tag: None,
            inner: None,
            handlers: vec![],
            mediator: Some(mediator),
        }
    }

    pub fn with_tag(mut self, tag: impl Into<String>) -> Self {
        self.node_tag = Some(tag.into());

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

        let node_id = *inner.system_id();
        let system_tag = self.node_tag.clone().unwrap_or_else(|| node_id.to_string());

        let types = handlers.build(self.node_tag);
        let handler_ref = RemoteHandler::new(&inner, &system_tag).await;
        let registry_ref = RemoteRegistry::new(&inner, &system_tag).await;

        let clients_ref = RemoteClientRegistry::new(&mut inner, &system_tag).await;
        let registry_ref_clone = registry_ref.clone();

        let mediator_ref = if let Some(mediator) = self.mediator {
            Some(
                inner
                    .new_actor(format!("PubSubMediator-{}", &system_tag), mediator, Tracked)
                    .await
                    .expect("unable to start mediator actor"),
            )
        } else {
            None
        };

        let mut core = RemoteSystemCore {
            node_id,
            inner,
            handler_ref,
            registry_ref,
            clients_ref,
            mediator_ref,
            types,
        };

        let inner = Arc::new(core.clone());
        let system = RemoteActorSystem { inner };
        core.inner.set_remote(system.clone());

        let system = RemoteActorSystem {
            inner: Arc::new(core),
        };

        registry_ref_clone
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

    pub fn build(self, tag: Option<String>) -> Arc<RemoteHandlerTypes> {
        let mut handler_types = HashMap::new();
        let mut actor_types = HashMap::new();

        for (k, v) in &self.handlers {
            let _ = handler_types.insert(v.id(), k.clone());
        }

        for (k, v) in &self.actors {
            let _ = actor_types.insert(v.id(), k.clone());
        }

        let node_tag = tag.map_or_else(|| format!("cluster-node-{}", Uuid::new_v4()), |t| t);
        Arc::new(RemoteHandlerTypes::new(
            node_tag,
            actor_types,
            handler_types,
            self.handlers,
            self.actors,
        ))
    }
}
