use crate::actor::message::{Envelope, Handler, Message};
use crate::actor::scheduler::ActorType::Tracked;
use crate::actor::system::ActorSystem;
use crate::actor::{
    new_actor_id, Actor, ActorFactory, ActorId, ActorRecipe, ActorRefErr, BoxedActorRef,
    CoreActorRef, LocalActorRef,
};
use crate::remote::actor::{BoxedActorHandler, BoxedMessageHandler};

use crate::actor::context::ActorContext;

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::sync::oneshot::Sender;

#[async_trait]
pub trait ActorHandler: 'static + Any + Sync + Send {
    async fn create(
        &self,
        actor_id: Option<String>,
        raw_recipe: Vec<u8>,
        supervisor_ctx: Option<&mut ActorContext>,
        system: Option<&ActorSystem>,
    ) -> Result<BoxedActorRef, ActorRefErr>;

    fn new_boxed(&self) -> BoxedActorHandler;

    fn id(&self) -> TypeId;

    fn actor_type_name(&self) -> &'static str;
}

#[async_trait]
pub trait ActorMessageHandler: Any {
    async fn handle_attempt(
        &self,
        actor: ActorId,
        buffer: &[u8],
        res: tokio::sync::oneshot::Sender<Result<Vec<u8>, ActorRefErr>>,
        attempt: usize,
    );

    async fn handle_direct(
        &self,
        actor: &BoxedActorRef,
        buffer: &[u8],
        res: Option<tokio::sync::oneshot::Sender<Result<Vec<u8>, ActorRefErr>>>,
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
    factory: F,
    marker: RemoteActorMarker<A>,
}

impl<A: Actor, F: ActorFactory> RemoteActorHandler<A, F>
where
    A: 'static + Send + Sync,
    F: 'static + Send + Sync,
{
    pub fn new(factory: F) -> RemoteActorHandler<A, F> {
        let marker = RemoteActorMarker::new();
        RemoteActorHandler { factory, marker }
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
        actor_id: Option<ActorId>,
        recipe: Vec<u8>,
        supervisor_ctx: Option<&mut ActorContext>,
        system: Option<&ActorSystem>,
    ) -> Result<BoxedActorRef, ActorRefErr> {
        let system = system.clone();
        let actor_id = actor_id.unwrap_or_else(|| new_actor_id());

        let recipe = F::Recipe::read_from_bytes(recipe);
        if let Some(recipe) = recipe {
            if let Ok(state) = self.factory.create(recipe).await {
                let actor_ref = if let Some(supervisor_ctx) = supervisor_ctx {
                    supervisor_ctx.spawn(actor_id, state).await
                } else {
                    system
                        .expect(
                            "ActorSystem ref should be provided if there is no `supervisor_ctx`",
                        )
                        .new_actor(actor_id, state, Tracked)
                        .await
                };

                actor_ref.map(|a| BoxedActorRef::from(a))
            } else {
                Err(ActorRefErr::ActorUnavailable)
            }
        } else {
            Err(ActorRefErr::ActorUnavailable)
        }
    }

    fn new_boxed(&self) -> BoxedActorHandler {
        Box::new(Self {
            marker: RemoteActorMarker::new(),
            factory: self.factory.clone(),
        })
    }

    fn id(&self) -> TypeId {
        self.type_id()
    }

    fn actor_type_name(&self) -> &'static str {
        A::type_name()
    }
}

struct ActorRefCache {
    inner: parking_lot::Mutex<HashMap<ActorId, BoxedActorRef>>,
}

impl ActorRefCache {
    pub fn new() -> Self {
        ActorRefCache {
            inner: parking_lot::Mutex::new(HashMap::new()),
        }
    }

    pub fn add(&self, actor_id: &ActorId, actor_ref: BoxedActorRef) {
        self.inner.lock().insert(actor_id.to_string(), actor_ref);
    }

    pub fn get(&self, actor_id: &ActorId) -> Option<BoxedActorRef> {
        self.inner.lock().get(actor_id).cloned()
    }
}

// TODO: We should apply some limiting to this, over time this could grow
//       - move to somewhere that wont require a mutex
lazy_static! {
    static ref ACTOR_REF_CACHE: ActorRefCache = ActorRefCache::new();
}

async fn get_actor_ref<A: Actor>(
    system: &ActorSystem,
    actor_id: &ActorId,
) -> Option<LocalActorRef<A>> {
    if let Some(boxed_ref) = ACTOR_REF_CACHE.get(actor_id) {
        let actor_ref = boxed_ref.as_actor();
        if let Some(actor_ref) = actor_ref {
            if actor_ref.is_valid() {
                return Some(actor_ref);
            }
        }
    }

    if let Some(actor_ref) = system.get_tracked_actor::<A>(actor_id.to_string()).await {
        ACTOR_REF_CACHE.add(actor_id, BoxedActorRef::from(actor_ref.clone()));

        Some(actor_ref)
    } else {
        None
    }
}

#[async_trait]
impl<A: Actor, M: Message> ActorMessageHandler for RemoteActorMessageHandler<A, M>
where
    A: Handler<M>,
{
    async fn handle_attempt(
        &self,
        actor_id: ActorId,
        buffer: &[u8],
        res: Sender<Result<Vec<u8>, ActorRefErr>>,
        attempt: usize,
    ) {
        let actor = get_actor_ref::<A>(&self.system, &actor_id).await;
        if let Some(actor) = actor {
            let envelope = M::from_envelope(Envelope::Remote(buffer.to_vec()));
            match envelope {
                Ok(m) => {
                    let result = actor.send(m).await;
                    if let Ok(result) = result {
                        match M::write_remote_result(result) {
                            Ok(buffer) => {
                                let send_res = res.send(Ok(buffer));
                                if let Err(_) = send_res {
                                    error!(target: "RemoteHandler", "failed to send result back to sender");
                                }
                            }

                            Err(e) => {
                                error!(target: "RemoteHandler", "failed to encode message result");
                                let _ = res.send(Err(ActorRefErr::Serialisation(e)));
                            }
                        }
                    }
                }

                Err(e) => {
                    error!(target: "RemoteHandler", "failed to decode message ({})", M::type_name());
                    let _ = res.send(Err(ActorRefErr::Deserialisation(e)));
                }
            };
        } else {
            const RETRY_DELAY_MILLIS: u64 = 10;
            const MAX_RETRIES: usize = 10;

            let next_attempt = attempt + 1;
            if next_attempt >= MAX_RETRIES {
                error!(
                    "actor={} not found, exceeded max retries (attempts={})",
                    &actor_id, attempt
                );

                let _ = res.send(Err(ActorRefErr::NotFound(actor_id)));
                return;
            }

            warn!(
                "actor={} not found, retrying in {}ms (attempts={}, max={})",
                &actor_id, RETRY_DELAY_MILLIS, attempt, MAX_RETRIES
            );

            tokio::time::sleep(Duration::from_millis(RETRY_DELAY_MILLIS)).await;
            self.handle_attempt(actor_id, buffer, res, next_attempt)
                .await;
        }
    }

    async fn handle_direct(
        &self,
        actor: &BoxedActorRef,
        buffer: &[u8],
        res: Option<Sender<Result<Vec<u8>, ActorRefErr>>>,
    ) {
        let actor_type = actor.actor_type();
        let actor = actor.as_actor::<A>();
        let envelope = M::from_envelope(Envelope::Remote(buffer.to_vec()));

        match (actor, envelope) {
            (Some(actor), Ok(message)) => match res {
                Some(res) => {
                    let result = actor
                        .send(message)
                        .await
                        .map(|result| M::write_remote_result(result));
                    match result {
                        Ok(Ok(result)) => {
                            if res.send(Ok(result)).is_err() {
                                error!(target: "RemoteHandler", "failed to send message")
                            } else {
                                trace!(target: "RemoteHandler", "handled message (actor_type={}, message_type={})", &actor_type, M::type_name());
                            }
                        }
                        Ok(Err(e)) => {
                            error!(target: "RemoteHandler", "failed to encode message result: {}", &e);
                            let _ = res.send(Err(ActorRefErr::Serialisation(e)));
                        }
                        Err(e) => {
                            error!(target: "RemoteHandler", "failed to send message");
                            let _ = res.send(Err(e));
                        }
                    }
                }
                None => {
                    trace!(target: "RemoteHandler", "no result consumer, notifying actor");
                    let _res = actor.notify(message);
                }
            },
            (_, Err(e)) => {
                error!("deserialisation error, {}", e);

                if let Some(res) = res {
                    let _ = res.send(Err(ActorRefErr::Deserialisation(e)));
                }
            }
            (None, _) => {
                error!(
                    "could not convert BoxedActorRef (inner={}) to LocalActorRef<{}>",
                    actor_type,
                    A::type_name()
                );

                // TODO: send notification back to `res` sender
            }
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

pub fn send_proto_result<M: protobuf::Message>(msg: M, res: tokio::sync::oneshot::Sender<Vec<u8>>)
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
