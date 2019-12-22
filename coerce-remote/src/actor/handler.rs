use crate::actor::message::{GetHandler, HandlerName, PopRequest, PushRequest};
use crate::actor::{BoxedHandler, RemoteHandler, RemoteRequest};
use coerce_rt::actor::context::ActorHandlerContext;
use coerce_rt::actor::message::{Handler, Message};
use coerce_rt::actor::Actor;

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
