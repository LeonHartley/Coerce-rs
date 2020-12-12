use crate::actor::message::{Handler, Message};
use crate::actor::system::ActorSystem;
use crate::actor::{Actor, Factory};
use crate::remote::actor::message::SetSystem;
use crate::remote::actor::{
    BoxedActorHandler, BoxedMessageHandler, RemoteClientRegistry, RemoteHandler,
    RemoteHandlerTypes, RemoteRegistry,
};
use crate::remote::codec::json::JsonCodec;
use crate::remote::handler::{RemoteActorHandler, RemoteActorMessageHandler};
use crate::remote::storage::activator::{ActorActivator, DefaultActorStore};
use crate::remote::storage::state::ActorStore;
use crate::remote::system::RemoteActorSystem;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

pub struct RemoteActorSystemBuilder {
    node_id: Option<Uuid>,
    inner: Option<ActorSystem>,
    handlers: Vec<HandlerFn>,
    store: Option<Box<dyn ActorStore + Sync + Send>>,
}

impl RemoteActorSystemBuilder {
    pub fn new() -> RemoteActorSystemBuilder {
        RemoteActorSystemBuilder {
            node_id: None,
            inner: None,
            handlers: vec![],
            store: None,
        }
    }

    pub fn with_node_id(mut self, node_id: Uuid) -> Self {
        self.node_id = Some(node_id);

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

    pub fn with_actor_system(mut self, ctx: ActorSystem) -> Self {
        self.inner = Some(ctx.clone());

        self
    }

    pub fn with_actor_store<S: ActorStore>(mut self, store: S) -> Self
    where
        S: 'static + Sync + Send,
    {
        self.store = Some(Box::new(store));

        self
    }

    pub async fn build(self) -> RemoteActorSystem {
        let mut inner = match self.inner {
            Some(ctx) => ctx,
            None => ActorSystem::current_system(),
        };

        let mut handlers = RemoteActorHandlerBuilder::new(inner.clone());

        self.handlers.into_iter().for_each(|h| {
            h(&mut handlers);
        });

        let types = handlers.build();
        let node_id = self.node_id.or_else(|| Some(Uuid::new_v4())).unwrap();
        let handler_ref = RemoteHandler::new(&mut inner).await;
        let registry_ref = RemoteRegistry::new(&mut inner).await;

        let clients_ref = RemoteClientRegistry::new(&mut inner).await;
        let mut registry_ref_clone = registry_ref.clone();

        let store = self.store.unwrap_or_else(|| Box::new(DefaultActorStore));
        let activator = ActorActivator::new(store);
        let system = RemoteActorSystem {
            node_id,
            inner,
            handler_ref,
            registry_ref,
            clients_ref,
            activator,
            types,
        };

        registry_ref_clone
            .send(SetSystem(system.clone()))
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
        let handler =
            RemoteActorMessageHandler::<A, M, _>::new(self.system.clone(), JsonCodec::new());
        self.handlers.insert(String::from(identifier), handler);

        self
    }

    pub fn with_actor<F: Factory>(&mut self, identifier: &'static str, factory: F) -> &mut Self
    where
        F: 'static + Factory + Send + Sync,
    {
        let handler = Box::new(RemoteActorHandler::<F::Actor, F, _>::new(
            self.system.clone(),
            factory,
            JsonCodec::new(),
        ));
        self.actors.insert(String::from(identifier), handler);

        self
    }

    pub fn build(self) -> Arc<RemoteHandlerTypes> {
        let mut handler_types = HashMap::new();
        let mut actor_types = HashMap::new();

        for (k, v) in &self.handlers {
            let _ = handler_types.insert(v.id(), k.clone());
        }

        for (k, v) in &self.actors {
            let _ = actor_types.insert(v.id(), k.clone());
        }

        Arc::new(RemoteHandlerTypes::new(
            actor_types,
            handler_types,
            self.handlers,
            self.actors,
        ))
    }
}
