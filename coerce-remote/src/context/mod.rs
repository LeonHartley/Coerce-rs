use crate::actor::{GetHandler, HandlerName, RemoteHandler};
use crate::codec::{MessageEncoder, RemoteHandlerMessage};
use coerce_rt::actor::context::ActorContext;
use coerce_rt::actor::message::Message;
use coerce_rt::actor::{Actor, ActorId, ActorRef};
use serde::Serialize;

use crate::context::builder::RemoteActorContextBuilder;

pub mod builder;

#[derive(Clone)]
pub struct RemoteActorContext {
    inner: ActorContext,
    handler_ref: ActorRef<RemoteHandler>,
}

impl RemoteActorContext {
    pub fn builder() -> RemoteActorContextBuilder {
        RemoteActorContextBuilder::new()
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
