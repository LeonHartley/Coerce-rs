use crate::actor::message::SetContext;
use crate::actor::{BoxedHandler, RemoteClientRegistry, RemoteHandler, RemoteRegistry};
use crate::codec::json::JsonCodec;
use crate::context::RemoteActorContext;
use crate::handler::{RemoteActorMessageHandler, RemoteMessageHandler};
use crate::storage::activator::ActorActivator;
use crate::storage::state::ActorStore;
use coerce_rt::actor::context::ActorContext;
use coerce_rt::actor::message::{Handler, Message};
use coerce_rt::actor::Actor;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashMap;
use uuid::Uuid;

pub struct RemoteActorContextBuilder {
    node_id: Option<Uuid>,
    inner: Option<ActorContext>,
    handlers: Vec<HandlerFn>,
    store: Option<Box<dyn ActorStore + Sync + Send>>,
}

impl RemoteActorContextBuilder {
    pub fn new() -> RemoteActorContextBuilder {
        RemoteActorContextBuilder {
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

    pub fn with_handlers<F>(mut self, f: F) -> Self
    where
        F: 'static + (FnOnce(&mut RemoteActorHandlerBuilder) -> &mut RemoteActorHandlerBuilder),
    {
        self.handlers.push(Box::new(f));

        self
    }

    pub fn with_actor_context(mut self, ctx: ActorContext) -> Self {
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

    pub async fn build(self) -> RemoteActorContext {
        let mut inner = match self.inner {
            Some(ctx) => ctx,
            None => ActorContext::current_context(),
        };

        let mut handlers = RemoteActorHandlerBuilder::new(inner.clone());

        self.handlers.into_iter().for_each(|h| {
            h(&mut handlers);
        });

        let handlers = handlers.build();

        let mut handler_types = HashMap::new();
        for (k, v) in &handlers {
            let _ = handler_types.insert(v.id(), k.clone());
        }

        let handler_ref = RemoteHandler::new(&mut inner, handlers, handler_types).await;

        let registry_ref = RemoteRegistry::new(&mut inner).await;
        let clients_ref = RemoteClientRegistry::new(&mut inner).await;
        let mut registry_ref_clone = registry_ref.clone();
        let activator = ActorActivator::new(self.store.expect("no actor store set"));

        let node_id = self.node_id.or_else(|| Some(Uuid::new_v4())).unwrap();
        let context = RemoteActorContext {
            node_id,
            inner,
            handler_ref,
            registry_ref,
            clients_ref,
            activator,
        };

        registry_ref_clone
            .send(SetContext(context.clone()))
            .await
            .expect("no context set");
        context
    }
}

pub(crate) type HandlerFn =
    Box<dyn (FnOnce(&mut RemoteActorHandlerBuilder) -> &mut RemoteActorHandlerBuilder)>;

pub struct RemoteActorHandlerBuilder {
    context: ActorContext,
    handlers: HashMap<String, Box<dyn RemoteMessageHandler + Send + Sync>>,
}

impl RemoteActorHandlerBuilder {
    pub fn new(context: ActorContext) -> RemoteActorHandlerBuilder {
        RemoteActorHandlerBuilder {
            handlers: HashMap::new(),
            context,
        }
    }

    pub fn with_handler<A: Actor, M: Message>(&mut self, identifier: &'static str) -> &mut Self
    where
        A: 'static + Handler<M> + Send + Sync,
        M: 'static + DeserializeOwned + Send + Sync,
        M::Result: Serialize + Send + Sync,
    {
        let handler =
            RemoteActorMessageHandler::<A, M, _>::new(self.context.clone(), JsonCodec::new());
        self.handlers.insert(String::from(identifier), handler);

        self
    }

    pub fn build(self) -> HashMap<String, BoxedHandler> {
        self.handlers
    }
}
