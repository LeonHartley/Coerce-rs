use crate::actor::BoxedHandler;
use crate::codec::{MessageDecoder, MessageEncoder, MessageCodec};
use coerce_rt::actor::context::ActorContext;
use coerce_rt::actor::message::{Handler, Message};
use coerce_rt::actor::{Actor, ActorId};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::any::{Any, TypeId};
use std::marker::PhantomData;

#[async_trait]
pub trait RemoteMessageHandler: Any {
    async fn handle(
        &self,
        actor: ActorId,
        buffer: &[u8],
        res: tokio::sync::oneshot::Sender<Vec<u8>>,
    );

    fn new_boxed(&self) -> BoxedHandler;

    fn id(&self) -> TypeId;
}

pub struct RemoteActorMessageMarker<A: Actor, M: Message>
where
    A: Send + Sync,
    M: Send + Sync,
    M::Result: Send + Sync,
{
    _m: PhantomData<M>,
    _a: PhantomData<A>,
}

impl<A: Actor, M: Message> RemoteActorMessageMarker<A, M>
where
    Self: Any,
    A: Send + Sync,
    M: Send + Sync,
    M::Result: Send + Sync,
{
    pub fn new() -> RemoteActorMessageMarker<A, M> {
        RemoteActorMessageMarker {
            _a: PhantomData,
            _m: PhantomData,
        }
    }

    pub fn id(&self) -> TypeId {
        self.type_id()
    }
}
pub struct RemoteActorMessageHandler<A: Actor, M: Message, C: MessageCodec>
where
    A: Send + Sync,
    M: DeserializeOwned + Send + Sync,
    M::Result: Serialize + Send + Sync,
{
    context: ActorContext,
    codec: C,
    marker: RemoteActorMessageMarker<A, M>,
}

impl<A: Actor, M: Message, C: MessageCodec> RemoteActorMessageHandler<A, M, C>
where
    A: 'static + Send + Sync,
    M: DeserializeOwned + 'static + Send + Sync,
    M::Result: Serialize + Send + Sync,
{
    pub fn new(context: ActorContext, codec: C) -> Box<RemoteActorMessageHandler<A, M, C>> {
        let marker = RemoteActorMessageMarker::new();
        Box::new(RemoteActorMessageHandler { context, codec, marker })
    }
}

#[async_trait]
impl<A: Actor, M: Message, C: MessageCodec> RemoteMessageHandler for RemoteActorMessageHandler<A, M, C>
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
        let mut context = self.context.clone();
        let actor = context.get_actor::<A>(actor_id).await;
        if let Some(mut actor) = actor {
            let message = M::decode(buffer.to_vec());
            match message {
                Some(m) => {
                    let result = actor.send(m).await;
                    if let Ok(result) = result {
                        match result.encode() {
                            Some(buffer) => {
                                if let Err(_) = res.send(buffer) {
                                    error!(target: "RemoteHandler", "failed to send message")
                                }
                            }
                            None => {
                                error!(target: "RemoteHandler", "failed to encode message result")
                            }
                        }
                    }
                }
                None => error!(target: "RemoteHandler", "failed to decode message"),
            };
        }
    }

    fn new_boxed(&self) -> BoxedHandler {
        Box::new(Self {
            context: self.context.clone(),
            marker: RemoteActorMessageMarker::new(),
        })
    }

    fn id(&self) -> TypeId {
        self.marker.id()
    }
}
