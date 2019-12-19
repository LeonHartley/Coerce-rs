use crate::actor::RemoteHandler;
use crate::context::RemoteActorContext;
use crate::handler::{RemoteActorMessageHandler, RemoteMessageHandler};
use coerce_rt::actor::context::ActorContext;
use coerce_rt::actor::message::{Handler, Message};
use coerce_rt::actor::Actor;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashMap;
use crate::codec::json::JsonCodec;

pub struct RemoteActorContextBuilder {
    inner: Option<ActorContext>,
    handlers: HashMap<String, Box<dyn RemoteMessageHandler + Send + Sync>>,
}

impl RemoteActorContextBuilder {
    pub fn new() -> RemoteActorContextBuilder {
        RemoteActorContextBuilder {
            inner: None,
            handlers: HashMap::new(),
        }
    }

    pub fn with_handler<A: Actor, M: Message>(mut self, identifier: &str) -> Self
    where
        A: 'static + Handler<M> + Send + Sync,
        M: 'static + DeserializeOwned + Send + Sync,
        M::Result: Serialize + Send + Sync,
    {
        let ctx = match &self.inner {
            Some(ctx) => ctx.clone(),
            None => ActorContext::current_context(),
        };

        let handler = RemoteActorMessageHandler::<A, M, _>::new(ctx, JsonCodec::new());

        self.handlers.insert(String::from(identifier), handler);

        self
    }

    pub fn with_actor_context(mut self, ctx: ActorContext) -> Self {
        self.inner = Some(ctx.clone());

        self
    }

    pub async fn build(self) -> RemoteActorContext {
        let mut inner = match self.inner {
            Some(ctx) => ctx,
            None => ActorContext::current_context(),
        };

        let mut handler_types = HashMap::new();
        for (k, v) in &self.handlers {
            let _ = handler_types.insert(v.id(), k.clone());
        }

        let handler_ref = RemoteHandler::new(&mut inner, self.handlers, handler_types).await;
        RemoteActorContext { inner, handler_ref }
    }
}
