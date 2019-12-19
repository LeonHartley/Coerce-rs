use crate::context::RemoteActorContext;
use coerce_rt::actor::message::{Handler, Message};
use coerce_rt::actor::{Actor, ActorId, ActorRefError};
use std::marker::PhantomData;

#[macro_use]
extern crate async_trait;

extern crate serde_json;

#[macro_use]
extern crate log;

extern crate tokio;
extern crate uuid;

pub mod actor;
pub mod codec;
pub mod context;
pub mod handler;
pub mod node;
pub mod transport;

pub struct RemoteActorRef<A: Actor>
where
    A: 'static + Sync + Send,
{
    id: ActorId,
    context: RemoteActorContext,
    _a: PhantomData<A>,
}

impl<A: Actor> RemoteActorRef<A>
where
    A: 'static + Sync + Send,
{
    pub fn new(id: ActorId, context: RemoteActorContext) -> RemoteActorRef<A> {
        RemoteActorRef {
            id,
            context,
            _a: PhantomData,
        }
    }

    pub async fn send<Msg: Message>(&mut self, _msg: Msg) -> Result<Msg::Result, ActorRefError>
    where
        Msg: 'static + Send + Sync,
        A: Handler<Msg>,
        Msg::Result: Send + Sync,
    {
        Err(ActorRefError::ActorUnavailable)
    }
}
