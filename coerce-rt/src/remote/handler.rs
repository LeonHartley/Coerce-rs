use crate::actor::message::{Handler, Message};
use crate::actor::remote::RemoteActorContext;
use crate::actor::{get_actor, Actor, ActorId, ActorRef};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashMap;
use std::marker::PhantomData;

#[async_trait]
pub trait RemoteMessageHandler {
    async fn handle(
        &self,
        actor: ActorId,
        buffer: &[u8],
        res: tokio::sync::oneshot::Sender<Vec<u8>>,
    );
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

impl<A: Actor, M: Message> RemoteActorMessageHandler<A, M>
where
    A: Send + Sync,
    M: DeserializeOwned + Send + Sync,
    M::Result: Serialize + Send + Sync,
{
    pub fn new() -> RemoteActorMessageHandler<A, M> {
        RemoteActorMessageHandler {
            _m: PhantomData,
            _a: PhantomData,
        }
    }
}

#[async_trait]
impl<A: Actor, M: Message> RemoteMessageHandler for RemoteActorMessageHandler<A, M>
where
    A: 'static + Handler<M> + Send + Sync,
    M: 'static + DeserializeOwned + Send + Sync,
    M::Result: Serialize + Send + Sync,
{
    async fn handle(
        &self,
        actor_id: ActorId,
        buffer: &[u8],
        res: tokio::sync::oneshot::Sender<Vec<u8>>,
    ) {
        let actor = get_actor::<A>(actor_id).await;
        if let Some(mut actor) = actor {
            let message = serde_json::from_slice::<M>(buffer);
            match message {
                Ok(m) => {
                    let result = actor.send(m).await;
                    if let Ok(result) = result {
                        res.send(serde_json::to_string(&result).unwrap().as_bytes().to_vec());
                    }
                }
                Err(e) => println!("decode failed"),
            };
        }
    }
}
