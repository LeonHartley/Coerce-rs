use crate::actor::message::{
    ClientWrite, GetHandler, HandlerName, PopRequest, PushRequest, RegisterClient,
};
use crate::actor::{RemoteHandler, RemoteRegistry, RemoteRequest, RemoteResponse};
use crate::codec::RemoteHandlerMessage;
use crate::context::builder::RemoteActorContextBuilder;
use crate::net::client::RemoteClientStream;
use crate::net::message::SessionEvent;
use crate::RemoteActorRef;
use coerce_rt::actor::context::ActorContext;
use coerce_rt::actor::message::Message;
use coerce_rt::actor::{Actor, ActorId, ActorRef};
use serde::Serialize;
use uuid::Uuid;

pub mod builder;

#[derive(Clone)]
pub struct RemoteActorContext {
    inner: ActorContext,
    handler_ref: ActorRef<RemoteHandler>,
    registry_ref: ActorRef<RemoteRegistry>,
}

impl RemoteActorContext {
    pub fn builder() -> RemoteActorContextBuilder {
        RemoteActorContextBuilder::new()
    }
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
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
        id: ActorId,
        message: M,
    ) -> Option<RemoteHandlerMessage<M>>
    where
        A: 'static + Send + Sync,
        M: 'static + Serialize + Send + Sync,
        M::Result: Send + Sync,
    {
        match self.handler_name::<A, M>().await {
            Some(handler_type) => Some(RemoteHandlerMessage {
                actor_id: id,
                handler_type,
                message,
            }),
            None => None,
        }
    }

    pub async fn register_client<T: RemoteClientStream>(&mut self, node_id: Uuid, client: T)
    where
        T: 'static + Sync + Send,
    {
        self.registry_ref
            .send(RegisterClient(node_id, client))
            .await
            .unwrap()
    }

    pub async fn send_message(&mut self, node_id: Uuid, message: SessionEvent) {
        self.registry_ref
            .send(ClientWrite(node_id, message))
            .await
            .unwrap()
    }

    pub async fn push_request(
        &mut self,
        id: Uuid,
        res_tx: tokio::sync::oneshot::Sender<RemoteResponse>,
    ) {
        self.handler_ref
            .send(PushRequest(id, RemoteRequest { res_tx }))
            .await;
    }

    pub async fn pop_request(
        &mut self,
        id: Uuid,
    ) -> Option<tokio::sync::oneshot::Sender<RemoteResponse>> {
        match self.handler_ref.send(PopRequest(id)).await {
            Ok(s) => s.map(|s| s.res_tx),
            Err(_) => None,
        }
    }

    pub fn inner(&mut self) -> &mut ActorContext {
        &mut self.inner
    }
}
