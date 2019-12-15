use crate::actor::message::{Handler, Message};
use crate::actor::{get_actor, Actor, ActorId, ActorRef};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashMap;
use std::marker::PhantomData;

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

pub struct RemoteActorContextBuilder {
    handlers: HashMap<String, Box<dyn RemoteMessageHandler>>,
}

#[async_trait]
pub trait RemoteMessageHandler {
    async fn handle(&self, actor: ActorId, buffer: &[u8]);
}

pub struct RemoteActorMessageHandler<A: Actor, M: Message>
where
    A: Send + Sync,
    M: DeserializeOwned + Send + Sync,
    M::Result: Serialize + Send + Sync,
{
    _m: PhantomData<M>,
    _a: PhantomData<A>,
}

#[async_trait]
impl<A: Actor, M: Message> RemoteMessageHandler for RemoteActorMessageHandler<A, M>
where
    A: 'static + Handler<M> + Send + Sync,
    M: 'static + DeserializeOwned + Send + Sync,
    M::Result: Serialize + Send + Sync,
{
    async fn handle(&self, actor_id: ActorId, buffer: &[u8]) {
        let actor = get_actor::<A>(actor_id).await;
        if let Some(mut actor) = actor {
            let message = serde_json::from_slice::<M>(buffer);
            match message {
                Ok(m) => {
                    println!("lool");
                    actor.send(m).await;
                }
                Err(e) => println!("decode failed"),
            };
        }
    }
}

impl RemoteActorContext {
    pub async fn handle(&mut self, identifier: String, actor_id: ActorId, buffer: &[u8]) {
        let handler = self.handlers.get(&identifier);
        if let Some(handler) = handler {
            handler.handle(actor_id, buffer).await;
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
        let handler: RemoteActorMessageHandler<A, M> = RemoteActorMessageHandler {
            _m: PhantomData,
            _a: PhantomData,
        };

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
