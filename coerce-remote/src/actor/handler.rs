use crate::actor::message::{
    ClientWrite, GetHandler, HandlerName, PopRequest, PushRequest, RegisterClient,
};
use crate::actor::{BoxedHandler, RemoteHandler, RemoteRegistry, RemoteRequest};
use crate::net::client::RemoteClientStream;
use coerce_rt::actor::context::ActorHandlerContext;
use coerce_rt::actor::message::{Handler, Message};
use coerce_rt::actor::Actor;
use std::hash::Hasher;

#[async_trait]
impl Handler<GetHandler> for RemoteHandler {
    async fn handle(
        &mut self,
        message: GetHandler,
        _ctx: &mut ActorHandlerContext,
    ) -> Option<BoxedHandler> {
        match self.handlers.get(&message.0) {
            Some(handler) => Some(handler.new_boxed()),
            None => None,
        }
    }
}

#[async_trait]
impl Handler<PushRequest> for RemoteHandler {
    async fn handle(&mut self, message: PushRequest, _ctx: &mut ActorHandlerContext) {
        self.requests.insert(message.0, message.1);
    }
}

#[async_trait]
impl Handler<PopRequest> for RemoteHandler {
    async fn handle(
        &mut self,
        message: PopRequest,
        _ctx: &mut ActorHandlerContext,
    ) -> Option<RemoteRequest> {
        self.requests.remove(&message.0)
    }
}

#[async_trait]
impl<A: Actor, M: Message> Handler<HandlerName<A, M>> for RemoteHandler
where
    A: 'static + Send + Sync,
    M: 'static + Send + Sync,
    M::Result: 'static + Sync + Send,
{
    async fn handle(
        &mut self,
        message: HandlerName<A, M>,
        _ctx: &mut ActorHandlerContext,
    ) -> Option<String> {
        match self.handler_types.get(&message.marker.id()) {
            Some(name) => Some(name.clone()),
            None => None,
        }
    }
}

#[async_trait]
impl<T: RemoteClientStream> Handler<RegisterClient<T>> for RemoteRegistry
where
    T: 'static + Sync + Send,
{
    async fn handle(&mut self, message: RegisterClient<T>, ctx: &mut ActorHandlerContext) {
        self.clients.insert(message.0, Box::new(message.1));

        trace!(target: "RemoteRegistry", "client {} registered", message.0);
    }
}

#[async_trait]
impl Handler<ClientWrite> for RemoteRegistry {
    async fn handle(&mut self, message: ClientWrite, _ctx: &mut ActorHandlerContext) {
        let client_id = message.0;
        let message = message.1;

        if let Some(mut client) = self.clients.get_mut(&client_id) {
            client.send(message).await;
            trace!(target: "RemoteRegistry", "writing data to client")
        } else {
            trace!(target: "RemoteRegistry", "client {} not found", &client_id);
        }
    }
}
