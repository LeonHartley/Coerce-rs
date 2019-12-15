use crate::actor::{ActorRef, Actor, ActorId};
use crate::remote::handler::{RemoteActorMessageHandler, RemoteMessageHandler};
use std::collections::HashMap;
use crate::actor::message::{Message, Handler};
use serde::de::DeserializeOwned;
use serde::Serialize;
use crate::remote::actor::RemoteRegistry;

pub struct RemoteActorContext {
    handler: ActorRef<RemoteRegistry>
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

        let handler = self.handlers.get(&identifier);
        if let Some(handler) = handler {
            handler.handle(actor_id, buffer, tx).await;
        };

        match rx.await {
            Ok(res) => Ok(res),
            Err(e) => Err(RemoteActorError::ActorUnavailable),
        }
    }
}

pub struct RemoteActorContextBuilder {
    handlers: HashMap<String, Box<dyn RemoteMessageHandler>>,
}

impl RemoteActorContextBuilder {
    pub fn with_handler<A: Actor, M: Message>(mut self, identifier: &'static str) -> Self
        where
            A: 'static + Handler<M> + Send + Sync,
            M: 'static + DeserializeOwned + Send + Sync,
            M::Result: Serialize + Send + Sync,
    {
        let handler = RemoteActorMessageHandler::<A, M>::new();

        self.handlers
            .insert(String::from(identifier), Box::new(handler));
        self
    }

    pub fn build(self) -> RemoteActorContext {
        RemoteActorContext {
            handler: ,
        }
    }
}
