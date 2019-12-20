use crate::actor::{BoxedHandler, RemoteHandler};
use crate::codec::json::JsonCodec;
use crate::context::RemoteActorContext;
use crate::handler::{RemoteActorMessageHandler, RemoteMessageHandler};
use coerce_rt::actor::context::ActorContext;
use coerce_rt::actor::message::{Handler, Message};
use coerce_rt::actor::Actor;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashMap;

pub struct RemoteActorContextBuilder {
    inner: Option<ActorContext>,
    handlers: Vec<HandlerFn>,
}

impl RemoteActorContextBuilder {
    pub fn new() -> RemoteActorContextBuilder {
        RemoteActorContextBuilder {
            inner: None,
            handlers: vec![],
        }
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

    pub async fn build(mut self) -> RemoteActorContext {
        let mut inner = match self.inner {
            Some(ctx) => ctx,
            None => ActorContext::current_context(),
        };

        let mut handlers = RemoteActorHandlerBuilder::new(inner.clone());

        self.handlers.into_iter().for_each(|mut h| {
            h(&mut handlers);
        });

        let handlers = handlers.build();

        let mut handler_types = HashMap::new();
        for (k, v) in &handlers {
            let _ = handler_types.insert(v.id(), k.clone());
        }

        let handler_ref = RemoteHandler::new(&mut inner, handlers, handler_types).await;
        RemoteActorContext { inner, handler_ref }
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
