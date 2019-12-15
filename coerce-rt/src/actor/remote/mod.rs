use crate::actor::message::{Handler, Message};
use crate::actor::remote::handler::{RemoteActorMessageHandler, RemoteMessageHandler};
use crate::actor::{get_actor, Actor, ActorId, ActorRef};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashMap;
use std::marker::PhantomData;

pub mod handler;

pub struct RemoteActorContext {
    handlers: HashMap<String, Box<dyn RemoteMessageHandler>>,
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
            handlers: self.handlers,
        }
    }
}
