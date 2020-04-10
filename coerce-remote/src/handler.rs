use crate::actor::BoxedMessageHandler;
use crate::codec::MessageCodec;
use crate::context::RemoteActorContext;
use crate::net::message::CreateActor;
use coerce_rt::actor::context::ActorContext;
use coerce_rt::actor::message::{Handler, Message};
use coerce_rt::actor::scheduler::ActorType::Tracked;
use coerce_rt::actor::{Actor, ActorId};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::any::{Any, TypeId};
use std::marker::PhantomData;

#[async_trait]
pub trait ActorHandler: Any {
    async fn create(
        &self,
        args: CreateActor,
        mut remote_ctx: RemoteActorContext,
        res: tokio::sync::oneshot::Sender<Vec<u8>>,
    );

    fn id(&self) -> TypeId;
}

#[async_trait]
pub trait ActorMessageHandler: Any {
    async fn handle(
        &self,
        actor: ActorId,
        mut remote_ctx: RemoteActorContext,
        buffer: &[u8],
        res: tokio::sync::oneshot::Sender<Vec<u8>>,
    );

    fn new_boxed(&self) -> BoxedMessageHandler;

    fn id(&self) -> TypeId;
}

pub struct RemoteActorMarker<A: Actor>
where
    A: Send + Sync,
{
    _a: PhantomData<A>,
}

impl<A: Actor> RemoteActorMarker<A>
where
    Self: Any,
    A: Send + Sync,
{
    pub fn new() -> RemoteActorMarker<A> {
        RemoteActorMarker { _a: PhantomData }
    }

    pub fn id(&self) -> TypeId {
        self.type_id()
    }
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
    C: Sized,
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
    C: Sized,
    A: 'static + Send + Sync,
    M: DeserializeOwned + 'static + Send + Sync,
    M::Result: Serialize + Send + Sync,
{
    pub fn new(context: ActorContext, codec: C) -> Box<RemoteActorMessageHandler<A, M, C>> {
        let marker = RemoteActorMessageMarker::new();
        Box::new(RemoteActorMessageHandler {
            context,
            codec,
            marker,
        })
    }
}

pub struct RemoteActorHandler<A: Actor>
where
    A: Send + Sync,
{
    context: ActorContext,
    marker: RemoteActorMarker<A>,
}

impl<A: Actor> RemoteActorHandler<A>
where
    A: 'static + Send + Sync,
{
    pub fn new(context: ActorContext) -> RemoteActorHandler<A> {
        let marker = RemoteActorMarker::new();
        RemoteActorHandler { context, marker }
    }
}

#[async_trait]
impl<A: Actor> ActorHandler for RemoteActorHandler<A>
where
    A: 'static + Sync + Send,
    A: DeserializeOwned,
{
    async fn create(
        &self,
        args: CreateActor,
        mut remote_ctx: RemoteActorContext,
        res: tokio::sync::oneshot::Sender<Vec<u8>>,
    ) {
        info!("attempting to create new actor")
    }

    fn id(&self) -> TypeId {
        self.type_id()
    }
}

#[async_trait]
impl<A: Actor, M: Message, C: MessageCodec> ActorMessageHandler
    for RemoteActorMessageHandler<A, M, C>
where
    C: 'static + Send + Sync,
    A: 'static + Handler<M> + Send + Sync,
    A: 'static + DeserializeOwned,
    M: 'static + DeserializeOwned + Send + Sync,
    M::Result: Serialize + Send + Sync,
{
    async fn handle(
        &self,
        actor_id: ActorId,
        mut remote_ctx: RemoteActorContext,
        buffer: &[u8],
        res: tokio::sync::oneshot::Sender<Vec<u8>>,
    ) {
        let mut context = self.context.clone();
        let mut actor = context.get_tracked_actor::<A>(actor_id.clone()).await;
        if actor.is_none() {
            // TODO: move this to the remote_ctx, `wake_actor` or something
            let raw_actor = remote_ctx.activator_mut().activate::<A>(&actor_id).await;

            if let Some(raw_actor) = raw_actor {
                match context.new_actor(actor_id, raw_actor, Tracked).await {
                    Ok(new_actor) => actor = Some(new_actor),
                    Err(e) => error!("failed to wake actor, error: {}", e),
                };
            }
        }

        if let Some(mut actor) = actor {
            let message = self.codec.decode_msg::<M>(buffer.to_vec());
            match message {
                Some(m) => {
                    let result = actor.send(m).await;
                    if let Ok(result) = result {
                        match self.codec.encode_msg(result) {
                            Some(buffer) => {
                                if res.send(buffer).is_err() {
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

    fn new_boxed(&self) -> BoxedMessageHandler {
        Box::new(Self {
            context: self.context.clone(),
            codec: self.codec.clone(),
            marker: RemoteActorMessageMarker::new(),
        })
    }

    fn id(&self) -> TypeId {
        self.marker.id()
    }
}
