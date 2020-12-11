use crate::actor::context::ActorSystem;
use crate::actor::message::{Handler, Message};
use crate::actor::scheduler::ActorType::Tracked;
use crate::actor::{Actor, ActorId, ActorState, FromActorState};
use crate::remote::actor::{BoxedActorHandler, BoxedMessageHandler};
use crate::remote::codec::MessageCodec;
use crate::remote::context::RemoteActorSystem;
use crate::remote::net::message::{ActorCreated, CreateActor};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::any::{Any, TypeId};
use std::marker::PhantomData;
use uuid::Uuid;

#[async_trait]
pub trait ActorHandler: Any {
    async fn create(
        &self,
        args: CreateActor,
        mut remote_ctx: RemoteActorSystem,
        res: tokio::sync::oneshot::Sender<Vec<u8>>,
    );

    fn new_boxed(&self) -> BoxedActorHandler;

    fn id(&self) -> TypeId;
}

#[async_trait]
pub trait ActorMessageHandler: Any {
    async fn handle(
        &self,
        actor: ActorId,
        mut remote_ctx: RemoteActorSystem,
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
    system: ActorSystem,
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
    pub fn new(system: ActorSystem, codec: C) -> Box<RemoteActorMessageHandler<A, M, C>> {
        let marker = RemoteActorMessageMarker::new();
        Box::new(RemoteActorMessageHandler {
            system,
            codec,
            marker,
        })
    }
}

pub struct RemoteActorHandler<A: Actor, C: MessageCodec>
where
    C: Send + Sync,
    A: Send + Sync,
{
    system: ActorSystem,
    codec: C,
    marker: RemoteActorMarker<A>,
}

impl<A: Actor, C: MessageCodec> RemoteActorHandler<A, C>
where
    A: 'static + Send + Sync,
    C: 'static + Send + Sync,
{
    pub fn new(system: ActorSystem, codec: C) -> RemoteActorHandler<A, C> {
        let marker = RemoteActorMarker::new();
        RemoteActorHandler {
            system,
            codec,
            marker,
        }
    }
}

#[async_trait]
impl<A: Actor, C: MessageCodec> ActorHandler for RemoteActorHandler<A, C>
where
    A: 'static + Sync + Send,
    A: DeserializeOwned,
    C: 'static + Send + Sync,
{
    async fn create(
        &self,
        args: CreateActor,
        remote_ctx: RemoteActorSystem,
        res: tokio::sync::oneshot::Sender<Vec<u8>>,
    ) {
        let mut system = self.system.clone();
        let actor_id = args
            .actor_id
            .unwrap_or_else(|| format!("{}", Uuid::new_v4()));

        if let Some(state) = A::try_from(ActorState {
            actor_id: actor_id.clone(),
            state: args.actor,
        }) {
            let actor_ref = system.new_actor(actor_id, state, Tracked).await;
            if let Ok(actor_ref) = actor_ref {
                let result = ActorCreated {
                    id: actor_ref.id,
                    node_id: remote_ctx.node_id(),
                };

                send_result(result, res, &self.codec)
            }
        }
    }

    fn new_boxed(&self) -> BoxedActorHandler {
        Box::new(Self {
            system: self.system.clone(),
            marker: RemoteActorMarker::new(),
            codec: self.codec.clone(),
        })
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
    M: 'static + DeserializeOwned + Send + Sync,
    M::Result: Serialize + Send + Sync,
{
    async fn handle(
        &self,
        actor_id: ActorId,
        mut remote_ctx: RemoteActorSystem,
        buffer: &[u8],
        res: tokio::sync::oneshot::Sender<Vec<u8>>,
    ) {
        let mut system = self.system.clone();
        let mut actor = system.get_tracked_actor::<A>(actor_id.clone()).await;

        if let Some(mut actor) = actor {
            let message = self.codec.decode_msg::<M>(buffer.to_vec());
            match message {
                Some(m) => {
                    let result = actor.send(m).await;
                    if let Ok(result) = result {
                        send_result(result, res, &self.codec)
                    }
                }
                None => error!(target: "RemoteHandler", "failed to decode message"),
            };
        }
    }

    fn new_boxed(&self) -> BoxedMessageHandler {
        Box::new(Self {
            system: self.system.clone(),
            codec: self.codec.clone(),
            marker: RemoteActorMessageMarker::new(),
        })
    }

    fn id(&self) -> TypeId {
        self.marker.id()
    }
}

fn send_result<M: Serialize, C: MessageCodec>(
    msg: M,
    res: tokio::sync::oneshot::Sender<Vec<u8>>,
    codec: &C,
) where
    M: 'static + Sync + Send,
    C: 'static + Sync + Send,
{
    match codec.encode_msg(msg) {
        Some(buffer) => {
            if res.send(buffer).is_err() {
                error!(target: "RemoteHandler", "failed to send message")
            }
        }
        None => error!(target: "RemoteHandler", "failed to encode message result"),
    }
}
