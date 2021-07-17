use crate::actor::message::{Handler, Message};
use crate::actor::system::ActorSystem;
use crate::actor::{Actor, Factory};
use crate::remote::actor::message::SetRemote;
use crate::remote::actor::{
    BoxedActorHandler, BoxedMessageHandler, RemoteClientRegistry, RemoteHandler,
    RemoteHandlerTypes, RemoteRegistry,
};
use crate::remote::handler::{RemoteActorHandler, RemoteActorMessageHandler};
use crate::remote::storage::activator::{ActorActivator, DefaultActorStore};
use crate::remote::storage::state::ActorStore;
use crate::remote::stream::mediator::StreamMediator;
use crate::remote::stream::system::SystemTopic;
use crate::remote::system::{RemoteActorSystem, RemoteSystemCore};
use futures::TryFutureExt;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

pub struct RemoteActorSystemBuilder {
    node_id: Option<Uuid>,
    node_tag: Option<String>,
    inner: Option<ActorSystem>,
    handlers: Vec<HandlerFn>,
    store: Option<Box<dyn ActorStore + Sync + Send>>,
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
            store: None,
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

    pub fn with_actor_store<S: ActorStore>(mut self, store: S) -> Self
    where
        S: 'static + Sync + Send,
    {
        self.store = Some(Box::new(store));

        self
    }

    pub fn with_distributed_streams<F>(mut self, f: F) -> Self
    where
        F: 'static + (FnOnce(&mut StreamMediator) -> &mut StreamMediator),
    {
        let mut mediator = if let Some(mediator) = &mut self.mediator {
            mediator
        } else {
            let mut mediator = StreamMediator::new();
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

        let types = handlers.build(self.node_tag);
        let node_id = *inner.system_id();
        let handler_ref = RemoteHandler::new(&inner).await;
        let registry_ref = RemoteRegistry::new(&inner).await;

        let clients_ref = RemoteClientRegistry::new(&mut inner).await;
        let mut registry_ref_clone = registry_ref.clone();

        let store = self.store.unwrap_or_else(|| Box::new(DefaultActorStore));
        let activator = ActorActivator::new(store);

        let mediator_ref = if let Some(mediator) = self.mediator {
            trace!("mediator set");
            Some(
                inner
                    .new_tracked_actor(mediator)
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
            activator,
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
            stream_mediator.send(SetRemote(system.clone()))
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
        A: 'static + Handler<M> + Send + Sync,
        M: 'static + DeserializeOwned + Send + Sync,
        M::Result: Serialize + Send + Sync,
    {
        let handler = RemoteActorMessageHandler::<A, M>::new(self.system.clone());
        self.handlers.insert(String::from(identifier), handler);

        self
    }

    pub fn with_actor<F: Factory>(&mut self, identifier: &'static str, factory: F) -> &mut Self
    where
        F: 'static + Factory + Send + Sync,
    {
        let handler = Box::new(RemoteActorHandler::<F::Actor, F>::new(
            self.system.clone(),
            factory,
        ));

        self.actors.insert(String::from(identifier), handler);
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
