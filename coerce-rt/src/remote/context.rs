use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message};
use crate::actor::{Actor, ActorId, ActorRef};
use crate::remote::actor::{GetHandler, RemoteHandler};
use crate::remote::handler::{RemoteActorMessageHandler, RemoteMessageHandler};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;

pub struct RemoteActorContext {
    inner: ActorContext,
    handler_ref: ActorRef<RemoteHandler>,
}

pub struct RemoteActorContextBuilder {
    inner: Option<ActorContext>,
    handlers: HashMap<String, Box<dyn RemoteMessageHandler + Send + Sync>>,
}

impl RemoteActorContext {
    pub fn builder() -> RemoteActorContextBuilder {
        RemoteActorContextBuilder {
            inner: None,
            handlers: HashMap::new(),
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum RemoteActorError {
    ActorUnavailable,
}

impl RemoteActorContext {
    pub async fn handle(
        &mut self,
        identifier: String,
        actor_id: ActorId,
        buffer: &[u8],
    ) -> Result<Vec<u8>, RemoteActorError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let handler = self.handler_ref.send(GetHandler(identifier)).await;

        if let Ok(Some(handler)) = handler {
            handler.handle(actor_id, buffer, tx).await;
        };

        match rx.await {
            Ok(res) => Ok(res),
            Err(_e) => Err(RemoteActorError::ActorUnavailable),
        }
    }

    pub fn inner(&mut self) -> &mut ActorContext {
        &mut self.inner
    }
}

impl RemoteActorContextBuilder {
    pub fn with_handler<A: Actor, M: Message>(mut self, identifier: &'static str) -> Self
    where
        A: 'static + Handler<M> + Send + Sync,
        M: 'static + DeserializeOwned + Send + Sync,
        M::Result: Serialize + Send + Sync,
    {
        let ctx = match &self.inner {
            Some(ctx) => ctx.clone(),
            None => ActorContext::current_context(),
        };

        self.handlers.insert(
            String::from(identifier),
            RemoteActorMessageHandler::<A, M>::new(ctx),
        );

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

        let handler_ref = RemoteHandler::new(&mut inner, self.handlers).await;
        RemoteActorContext { inner, handler_ref }
    }
}
