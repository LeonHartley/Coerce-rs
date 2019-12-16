use crate::actor::message::{Handler, Message};
use crate::actor::{Actor, ActorId, ActorRef};
use crate::remote::actor::{GetHandler, RemoteHandler, RemoteRegistry};
use crate::remote::handler::{RemoteActorMessageHandler, RemoteMessageHandler};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashMap;

pub struct RemoteActorContext {
    handler_ref: ActorRef<RemoteHandler>,
    registry_ref: ActorRef<RemoteRegistry>,
}

pub struct RemoteActorContextBuilder {
    handlers: HashMap<String, Box<dyn RemoteMessageHandler + Send + Sync>>,
}

impl RemoteActorContext {
    pub fn builder() -> RemoteActorContextBuilder {
        RemoteActorContextBuilder {
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
            Err(e) => Err(RemoteActorError::ActorUnavailable),
        }
    }
}

impl RemoteActorContextBuilder {
    pub fn with_handler<A: Actor, M: Message>(mut self, identifier: &'static str) -> Self
    where
        A: 'static + Handler<M> + Send + Sync,
        M: 'static + DeserializeOwned + Send + Sync,
        M::Result: Serialize + Send + Sync,
    {
        self.handlers.insert(
            String::from(identifier),
            RemoteActorMessageHandler::<A, M>::new(),
        );
        self
    }

    pub async fn build(self) -> RemoteActorContext {
        RemoteActorContext {
            handler_ref: RemoteHandler::new(self.handlers).await,
            registry_ref: RemoteRegistry::new().await,
        }
    }
}
