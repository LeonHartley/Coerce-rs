use crate::actor::{new_actor, Actor, ActorRef};
use crate::remote::handler::RemoteMessageHandler;
use std::collections::HashMap;
use crate::actor::message::{Message, Handler};
use crate::actor::context::ActorHandlerContext;

pub struct RemoteRegistry {}

pub struct RemoteHandler {
    handlers: HashMap<String, Box<dyn RemoteMessageHandler + Send + Sync>>,
}

impl Actor for RemoteRegistry {}

impl Actor for RemoteHandler {}

impl RemoteRegistry {
    pub async fn new() -> ActorRef<RemoteRegistry> {
        new_actor(RemoteRegistry {}).await.unwrap()
    }
}

impl RemoteHandler {
    pub async fn new(
        handlers: HashMap<String, Box<dyn RemoteMessageHandler + Send + Sync>>,
    ) -> ActorRef<RemoteHandler> {
        new_actor(RemoteHandler { handlers }).await.unwrap()
    }
}

pub struct GetHandler(pub String);

impl Message for GetHandler {
    type Result = Option<Box<dyn RemoteMessageHandler + Send + Sync>>;
}

#[async_trait]
impl Handler<GetHandler> for RemoteHandler {
    async fn handle(&mut self, message: GetHandler, ctx: &mut ActorHandlerContext) -> Option<Box<dyn RemoteMessageHandler + Send + Sync>> {
        match self.handlers.get(&message.0) {
            Some(handler) => Some(handler.new_boxed()),
            None => None
        }
    }
}
