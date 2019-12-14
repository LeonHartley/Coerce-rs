use crate::actor::message::{Handler, Message};
use crate::actor::{Actor, ActorRef};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::marker::PhantomData;

struct RemoteActorContext {}

impl RemoteActorContext {
    pub fn builder() -> RemoteActorContextBuilder {
        RemoteActorContextBuilder {}
    }
}

struct RemoteActorContextBuilder {}

trait RemoteMessageHandler<A: Actor>
where
    A: Send + Sync,
{
    fn handle(&mut self, actor: &mut A, buffer: Vec<u8>);
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

impl<A: Actor, M: Message> RemoteMessageHandler<A> for RemoteActorMessageHandler<A, M>
where
    A: Handler<M> + Send + Sync,
    M: DeserializeOwned + Send + Sync,
    M::Result: Serialize + Send + Sync,
{
    fn handle(&mut self, actor: ActorRef<A>, buffer: Vec<u8>) {
        let buffer = buffer;
        let message = serde_json::from_slice::<M>(buffer.as_slice());
        match message {
            Ok(m) => println!("success!:D"),
            Err(e) => println!("decode failed"),
        };
    }
}

impl RemoteActorContextBuilder {
    pub fn with_handler<A: Actor, M: Message>(&mut self, identifier: &'static str) -> &mut Self
    where
        A: Handler<M> + Send + Sync,
        M: DeserializeOwned + Send + Sync,
        M::Result: Serialize + Send + Sync,
    {
        let handler: RemoteActorMessageHandler<A, M> = RemoteActorMessageHandler {
            _m: PhantomData,
            _a: PhantomData,
        };

        self
    }
}
