use coerce_rt::actor::context::ActorContext;
use coerce_rt::actor::message::{Handler, Message};
use coerce_rt::actor::{Actor, ActorId, ActorRef};
use crate::actor::{GetHandler, HandlerName, RemoteHandler};
use crate::codec::{MessageEncoder, RemoteHandlerMessage};
use crate::handler::{RemoteActorMessageHandler, RemoteMessageHandler};
use serde::de::DeserializeOwned;
use serde::Serialize;

use std::collections::HashMap;

#[derive(Clone)]
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

    pub async fn handler_name<A: Actor, M: Message>(&mut self) -> Option<String>
    where
        A: 'static + Send + Sync,
        M: 'static + Send + Sync,
        M::Result: Send + Sync,
    {
        self.handler_ref
            .send(HandlerName::<A, M>::new())
            .await
            .unwrap()
    }

    pub async fn create_message<A: Actor, M: Message>(
        &mut self,
        actor_ref: &ActorRef<A>,
        message: M,
    ) -> Option<RemoteHandlerMessage>
    where
        A: 'static + Send + Sync,
        M: 'static + Serialize + Send + Sync,
        M::Result: Send + Sync,
    {
        match self.handler_name::<A, M>().await {
            Some(handler_type) => Some(RemoteHandlerMessage {
                actor_id: actor_ref.id.clone(),
                handler_type,
                message: match message.encode() {
                    Some(s) => match String::from_utf8(s) {
                        Ok(msg) => msg,
                        Err(_e) => return None,
                    },
                    None => return None,
                },
            }),
            None => None,
        }
    }

    pub fn inner(&mut self) -> &mut ActorContext {
        &mut self.inner
    }
}

impl RemoteActorContextBuilder {
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

        let handler = RemoteActorMessageHandler::<A, M>::new(ctx);

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
