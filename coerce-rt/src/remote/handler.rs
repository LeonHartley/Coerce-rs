use crate::actor::message::{Handler, Message};
use crate::actor::{get_actor, Actor, ActorId};
use crate::remote::actor::BoxedHandler;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::marker::PhantomData;

#[async_trait]
pub trait RemoteMessageHandler {
    async fn handle(
        &self,
        actor: ActorId,
        buffer: &[u8],
        res: tokio::sync::oneshot::Sender<Vec<u8>>,
    );

    fn new_boxed(&self) -> BoxedHandler;
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
    pub fn new() -> Box<RemoteActorMessageHandler<A, M>> {
        Box::new(RemoteActorMessageHandler {
            _m: PhantomData,
            _a: PhantomData,
        })
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
                        match serde_json::to_string(&result) {
                            Ok(msg) => {
                                if let Err(_) = res.send(msg.as_bytes().to_vec()) {
                                    error!(target: "RemoteHandler", "failed to send message")
                                }
                            }
                            Err(_e) => {
                                error!(target: "RemoteHandler", "failed to encode message result")
                            }
                        }
                    }
                }
                Err(e) => error!(target: "RemoteHandler", "failed to decode message, {}", e),
            };
        }
    }

    fn new_boxed(&self) -> BoxedHandler {
        Box::new(Self {
            _a: PhantomData,
            _m: PhantomData,
        })
    }
}
