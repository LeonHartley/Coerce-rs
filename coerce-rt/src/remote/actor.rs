use crate::actor::context::ActorHandlerContext;
use crate::actor::message::{Handler, Message};
use crate::actor::{new_actor, Actor, ActorRef};
use crate::remote::handler::RemoteMessageHandler;
use std::collections::HashMap;

pub(crate) type BoxedHandler = Box<dyn RemoteMessageHandler + Send + Sync>;

pub struct RemoteRegistry {}

pub struct RemoteHandler {
    handlers: HashMap<String, BoxedHandler>,
}

impl Actor for RemoteRegistry {}

impl Actor for RemoteHandler {}

impl RemoteRegistry {
    pub async fn new() -> ActorRef<RemoteRegistry> {
        new_actor(RemoteRegistry {}).await.unwrap()
    }
}

impl RemoteHandler {
    pub async fn new(handlers: HashMap<String, BoxedHandler>) -> ActorRef<RemoteHandler> {
        new_actor(RemoteHandler { handlers }).await.unwrap()
    }
}

pub struct GetHandler(pub String);

impl Message for GetHandler {
    type Result = Option<BoxedHandler>;
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
