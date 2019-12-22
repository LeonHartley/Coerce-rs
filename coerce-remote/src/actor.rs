use crate::handler::{RemoteActorMessageMarker, RemoteMessageHandler};
use crate::net::client::{RemoteClient, RemoteClientStream};
use coerce_rt::actor::context::{ActorContext, ActorHandlerContext};
use coerce_rt::actor::message::{Handler, Message};
use coerce_rt::actor::{Actor, ActorRef};
use std::any::TypeId;
use std::collections::HashMap;
use uuid::Uuid;

pub(crate) type BoxedHandler = Box<dyn RemoteMessageHandler + Send + Sync>;

pub struct RemoteRegistry {
    clients: HashMap<Uuid, Box<dyn RemoteClientStream + Sync + Send>>,
}

pub struct RemoteHandler {
    handler_types: HashMap<TypeId, String>,
    handlers: HashMap<String, BoxedHandler>,
    requests: HashMap<Uuid, RemoteRequest>,
}

pub struct RemoteRequest {
    pub res_tx: tokio::sync::oneshot::Sender<Vec<u8>>,
}

impl Actor for RemoteRegistry {}

impl Actor for RemoteHandler {}

impl RemoteRegistry {
    pub async fn new(ctx: &mut ActorContext) -> ActorRef<RemoteRegistry> {
        ctx.new_anon_actor(RemoteRegistry {
            clients: HashMap::new(),
        })
        .await
        .expect("RemoteRegistry")
    }
}

impl RemoteHandler {
    pub async fn new(
        ctx: &mut ActorContext,
        handlers: HashMap<String, BoxedHandler>,
        handler_types: HashMap<TypeId, String>,
    ) -> ActorRef<RemoteHandler> {
        ctx.new_anon_actor(RemoteHandler {
            handler_types,
            handlers,
            requests: HashMap::new(),
        })
        .await
        .expect("RemoteHandler")
    }
}

pub struct HandlerName<A: Actor, M: Message>
where
    A: 'static + Send + Sync,
    M: 'static + Send + Sync,
    M::Result: 'static + Sync + Send,
{
    marker: RemoteActorMessageMarker<A, M>,
}

impl<A: Actor, M: Message> HandlerName<A, M>
where
    A: 'static + Send + Sync,
    M: 'static + Send + Sync,
    M::Result: 'static + Send + Sync,
{
    pub fn new() -> HandlerName<A, M> {
        HandlerName {
            marker: RemoteActorMessageMarker::new(),
        }
    }
}

impl<A: Actor, M: Message> Message for HandlerName<A, M>
where
    A: 'static + Send + Sync,
    M: 'static + Send + Sync,
    M::Result: 'static + Send + Sync,
{
    type Result = Option<String>;
}

pub struct GetHandler(pub String);

impl Message for GetHandler {
    type Result = Option<BoxedHandler>;
}

pub struct PushRequest(pub Uuid, pub RemoteRequest);

impl Message for PushRequest {
    type Result = ();
}

pub struct PopRequest(pub Uuid);

impl Message for PopRequest {
    type Result = Option<RemoteRequest>;
}

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
