use crate::actor::message::{Envelope, Handler, Message};
use crate::actor::scheduler::ActorType::Tracked;
use crate::actor::system::ActorSystem;
use crate::actor::{new_actor_id, Actor, ActorFactory, ActorId, ActorRecipe};
use crate::remote::actor::{BoxedActorHandler, BoxedMessageHandler};
use crate::remote::net::proto::protocol::{ActorAddress, CreateActor};
use crate::remote::system::RemoteActorSystem;

use serde::de::DeserializeOwned;
use serde::Serialize;
use std::any::{Any, TypeId};
use std::marker::PhantomData;

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

pub struct RemoteActorMessageMarker<A: Actor, M: Message> {
    _m: PhantomData<M>,
    _a: PhantomData<A>,
}

impl<A: Actor, M: Message> RemoteActorMessageMarker<A, M> {
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

pub struct RemoteActorMessageHandler<A: Actor, M: Message> {
    system: ActorSystem,
    _marker: RemoteActorMessageMarker<A, M>,
}

impl<A: Actor, M: Message> RemoteActorMessageHandler<A, M> {
    pub fn new(system: ActorSystem) -> Box<RemoteActorMessageHandler<A, M>> {
        let _marker = RemoteActorMessageMarker::new();
        Box::new(RemoteActorMessageHandler { system, _marker })
    }
}

pub struct RemoteActorHandler<A: Actor, F: ActorFactory>
where
    F: Send + Sync,
    A: Send + Sync,
{
    system: ActorSystem,
    factory: F,
    marker: RemoteActorMarker<A>,
}

impl<A: Actor, F: ActorFactory> RemoteActorHandler<A, F>
where
    A: 'static + Send + Sync,
    F: 'static + Send + Sync,
{
    pub fn new(system: ActorSystem, factory: F) -> RemoteActorHandler<A, F> {
        let marker = RemoteActorMarker::new();
        RemoteActorHandler {
            system,
            factory,
            marker,
        }
    }
}

#[async_trait]
impl<A: Actor, F: ActorFactory<Actor = A>> ActorHandler for RemoteActorHandler<A, F>
where
    A: 'static + Sync + Send,
    F: 'static + Send + Sync,
{
    async fn create(
        &self,
        args: CreateActor,
        remote_ctx: RemoteActorSystem,
        res: tokio::sync::oneshot::Sender<Vec<u8>>,
    ) {
        let system = self.system.clone();
        let actor_id = if !args.actor_id.is_empty() {
            args.actor_id
        } else {
            new_actor_id()
        };

        let recipe = F::Recipe::read_from_bytes(args.recipe);
        if let Some(recipe) = recipe {
            if let Ok(state) = self.factory.create(recipe).await {
                let actor_ref = system.new_actor(actor_id, state, Tracked).await;
                if let Ok(actor_ref) = actor_ref {
                    let result = ActorAddress {
                        actor_id: actor_ref.id,
                        node_id: remote_ctx.node_id().to_string(),
                        ..ActorAddress::default()
                    };

                    info!(target: "RemoteHandler", "sending created actor ref");
                    send_proto_result(result, res)
                }
            }
        }
    }

    fn new_boxed(&self) -> BoxedActorHandler {
        Box::new(Self {
            system: self.system.clone(),
            marker: RemoteActorMarker::new(),
            factory: self.factory.clone(),
        })
    }

    fn id(&self) -> TypeId {
        self.type_id()
    }
}

#[async_trait]
impl<A: Actor, M: Message> ActorMessageHandler for RemoteActorMessageHandler<A, M>
where
    A: Handler<M>,
{
    async fn handle(
        &self,
        actor_id: ActorId,
        buffer: &[u8],
        res: tokio::sync::oneshot::Sender<Vec<u8>>,
    ) {
        let system = self.system.clone();
        let actor = system.get_tracked_actor::<A>(actor_id.clone()).await;

        if let Some(actor) = actor {
            let envelope = M::from_envelope(Envelope::Remote(buffer.to_vec()));
            match envelope {
                Ok(m) => {
                    let result = actor.send(m).await;
                    if let Ok(result) = result {
                        match M::write_remote_result(result) {
                            Ok(buffer) => {
                                if res.send(buffer).is_err() {
                                    error!(target: "RemoteHandler", "failed to send message")
                                }
                            }

                            Err(_) => {
                                error!(target: "RemoteHandler", "failed to encode message result")
                            }
                        }
                    }
                }
                Err(_) => error!(target: "RemoteHandler", "failed to decode message"),
            };
        }
    }

    fn new_boxed(&self) -> BoxedMessageHandler {
        Box::new(Self {
            system: self.system.clone(),
            _marker: RemoteActorMessageMarker::new(),
        })
    }

    fn id(&self) -> TypeId {
        self._marker.id()
    }
}

fn send_proto_result<M: protobuf::Message>(msg: M, res: tokio::sync::oneshot::Sender<Vec<u8>>)
where
    M: 'static + Sync + Send,
{
    match msg.write_to_bytes() {
        Ok(buffer) => {
            if res.send(buffer).is_err() {
                error!(target: "RemoteHandler", "failed to send message")
            }
        }
        Err(e) => error!(target: "RemoteHandler", "failed to encode message result - {}", e),
    }
}
