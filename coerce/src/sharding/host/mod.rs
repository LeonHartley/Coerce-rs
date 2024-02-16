use crate::actor::context::ActorContext;
use crate::actor::message::{EnvelopeType, Handler, Message, MessageUnwrapErr, MessageWrapErr};
use crate::actor::{Actor, ActorId, ActorRef, IntoActorId, LocalActorRef};
use crate::remote::system::{NodeId, RemoteActorSystem};
use crate::remote::RemoteActorRef;
use crate::sharding::coordinator::allocation::DefaultAllocator;
use crate::sharding::coordinator::{ShardCoordinator, ShardId};
use crate::sharding::host::request::{handle_request, EntityRequest};
use crate::sharding::proto::sharding as proto;
use crate::sharding::shard::Shard;
use protobuf::Message as ProtoMessage;

use crate::remote::actor::BoxedActorHandler;
use crate::remote::heartbeat::Heartbeat;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};

use crate::actor::scheduler::ActorType;
use crate::sharding::coordinator::factory::CoordinatorFactory;
use crate::singleton::factory::SingletonFactory;
use crate::singleton::Singleton;
use uuid::Uuid;

pub mod request;
pub mod stats;

pub enum ShardState {
    Starting {
        actor_ref: LocalActorRef<Shard>,
        request_buffer: Vec<EntityRequest>,
        stop_requested: Option<StopRequested>,
    },
    Ready(LocalActorRef<Shard>),
    Stopping,
}

pub struct StopRequested {
    origin_node_id: NodeId,
    request_id: Uuid,
}

pub struct ShardHost {
    shard_entity: String,
    actor_handler: BoxedActorHandler,
    hosted_shards: HashMap<ShardId, ShardState>,
    remote_shards: HashMap<ShardId, ActorRef<Shard>>,
    requests_pending_shard_allocation: HashMap<ShardId, Vec<EntityRequest>>,
    allocator: Box<dyn ShardAllocator>,
    coordinator: Option<Singleton<ShardCoordinator, CoordinatorFactory>>,
}

impl ShardHost {
    pub fn actor_id(entity_type: &str, node_id: NodeId) -> ActorId {
        format!("shard-host-{}-{}", &entity_type, node_id).into_actor_id()
    }

    pub fn remote_ref(
        entity_type: &str,
        node_id: NodeId,
        remote: &RemoteActorSystem,
    ) -> ActorRef<Self> {
        RemoteActorRef::<ShardHost>::new(
            Self::actor_id(entity_type, node_id),
            node_id,
            remote.clone(),
        )
        .into()
    }
}

pub struct Init(pub Singleton<ShardCoordinator, CoordinatorFactory>);

impl Message for Init {
    type Result = ();
}

pub trait ShardAllocator: 'static + Send + Sync {
    fn allocate(&mut self, actor_id: &ActorId) -> ShardId;
}

#[async_trait]
impl Actor for ShardHost {
    async fn started(&mut self, ctx: &mut ActorContext) {
        Heartbeat::register(ctx.boxed_actor_ref(), ctx.system().remote());
    }
}

impl ShardHost {
    pub fn new(
        shard_entity: String,
        actor_handler: BoxedActorHandler,
        allocator: Option<Box<dyn ShardAllocator>>,
    ) -> ShardHost {
        ShardHost {
            shard_entity,
            actor_handler,
            hosted_shards: Default::default(),
            remote_shards: Default::default(),
            requests_pending_shard_allocation: Default::default(),
            allocator: allocator.map_or_else(
                || Box::new(DefaultAllocator::default()) as Box<dyn ShardAllocator>,
                |s| s,
            ),
            coordinator: None,
        }
    }

    pub fn get_coordinator(&self) -> Singleton<ShardCoordinator, CoordinatorFactory> {
        self.coordinator
            .as_ref()
            .expect("coordinator singleton reference")
            .clone()
    }
}

#[async_trait]
impl Handler<Init> for ShardHost {
    async fn handle(&mut self, message: Init, ctx: &mut ActorContext) {
        self.coordinator = Some(message.0);
    }
}

pub struct GetCoordinator;

impl Message for GetCoordinator {
    type Result = Singleton<ShardCoordinator, CoordinatorFactory>;
}

pub struct ShardAllocated(pub ShardId, pub NodeId);

pub struct ShardReallocating(pub ShardId);

pub struct ShardReady(pub ShardId, pub LocalActorRef<Shard>);

impl Message for ShardReady {
    type Result = ();
}

pub struct ShardStopped {
    shard_id: ShardId,
    stopped_successfully: bool,
}

pub struct StopShard {
    pub shard_id: ShardId,
    pub origin_node_id: NodeId,
    pub request_id: Uuid,
}

pub struct GetShards;

pub struct HostedShards {
    shards: Vec<ShardId>,
}

impl ShardState {
    pub fn actor_ref(&self) -> Option<&LocalActorRef<Shard>> {
        match self {
            ShardState::Ready(actor_ref) => Some(actor_ref),
            ShardState::Starting { actor_ref, .. } => Some(actor_ref),
            _ => None,
        }
    }
}

#[async_trait]
impl Handler<GetCoordinator> for ShardHost {
    async fn handle(
        &mut self,
        _message: GetCoordinator,
        _: &mut ActorContext,
    ) -> Singleton<ShardCoordinator, CoordinatorFactory> {
        self.coordinator.as_ref().unwrap().clone()
    }
}

#[async_trait]
impl Handler<ShardAllocated> for ShardHost {
    async fn handle(&mut self, message: ShardAllocated, ctx: &mut ActorContext) {
        let remote = ctx.system().remote();

        let shard_id = message.0;
        let node_id = message.1;
        let shard_actor_id = shard_actor_id(&self.shard_entity, shard_id);

        if node_id == remote.node_id() {
            let _ = self.remote_shards.remove(&shard_id);

            let self_ref = self.actor_ref(ctx);

            let hosted_shard_entry = self.hosted_shards.entry(shard_id);
            match hosted_shard_entry {
                Entry::Occupied(_) => {
                    // already allocated locally
                    debug!("local shard#{} already allocated", message.0);
                }

                Entry::Vacant(hosted_shard_entry) => {
                    let system = ctx.system().clone();

                    let handler = self.actor_handler.new_boxed();
                    let shard = Shard::new(shard_id, handler, true, self_ref);
                    let actor_ref = system
                        .new_actor_deferred(shard_actor_id, shard, ActorType::Tracked)
                        .await;

                    ctx.attach_child_ref(actor_ref.clone().into());

                    hosted_shard_entry.insert(ShardState::Starting {
                        actor_ref,
                        request_buffer: vec![],
                        stop_requested: None,
                    });

                    debug!("local shard#{} allocated to node={}", message.0, message.1);
                }
            }
        } else {
            if let Some(shard) = self.hosted_shards.remove(&shard_id) {
                debug!(
                    "shard#{} was allocated locally but now allocated remotely on node={}, stopping shard",
                    shard_id, node_id
                );

                if let Some(shard_actor) = shard.actor_ref() {
                    let _ = shard_actor.notify_stop();
                }
            }

            let shard_actor =
                RemoteActorRef::new(shard_actor_id, node_id, ctx.system().remote_owned()).into();

            debug!("remote shard#{} allocated on node={}", message.0, message.1);
            self.remote_shards.insert(shard_id, shard_actor);
        }

        if let Some(buffered_requests) = self.requests_pending_shard_allocation.remove(&shard_id) {
            debug!(
                "processing {} buffered requests for shard={}",
                buffered_requests.len(),
                shard_id
            );

            for request in buffered_requests {
                self.handle(request, ctx).await;
            }
        }
    }
}

#[async_trait]
impl Handler<ShardReady> for ShardHost {
    async fn handle(&mut self, message: ShardReady, ctx: &mut ActorContext) {
        let shard_id = message.0;
        let actor_ref = message.1;

        let shard_state = self
            .hosted_shards
            .insert(shard_id, ShardState::Ready(actor_ref.clone()));

        ctx.attach_child_ref(actor_ref.clone().into());

        match shard_state {
            Some(ShardState::Starting {
                request_buffer,
                stop_requested,
                ..
            }) => {
                if let Some(stop_requested) = stop_requested {
                    // TODO: move the request buffer over to `requests_pending_shard_allocation`
                    self.stop_shard(shard_id, actor_ref, ctx, Some(stop_requested));
                } else {
                    let mut shard_state = ShardState::Ready(actor_ref);
                    for request in request_buffer {
                        handle_request(request, shard_id, &mut shard_state);
                    }
                }
            }

            _ => {}
        }
    }
}

#[async_trait]
impl Handler<ShardReallocating> for ShardHost {
    async fn handle(&mut self, message: ShardReallocating, _ctx: &mut ActorContext) {
        let _ = self.remote_shards.remove(&message.0);
    }
}

#[async_trait]
impl Handler<StopShard> for ShardHost {
    async fn handle(&mut self, message: StopShard, ctx: &mut ActorContext) {
        let shard_id = message.shard_id;
        let shard_entry = self.hosted_shards.entry(shard_id);
        let stop_request = StopRequested {
            origin_node_id: message.origin_node_id,
            request_id: message.request_id,
        };

        match shard_entry {
            Entry::Occupied(mut shard_entry) => {
                // TODO: is this correct? if the status is starting, should we not update that rather than re-inserting `Stopping`?
                match shard_entry.insert(ShardState::Stopping) {
                    ShardState::Starting {
                        mut stop_requested, ..
                    } => {
                        // actor has requested to be started so we can't stop it yet, once the actor
                        // has finished starting, we can then stop it.
                        stop_requested = Some(stop_request);
                    }

                    ShardState::Ready(actor_ref) => {
                        self.stop_shard(shard_id, actor_ref, ctx, Some(stop_request))
                    }

                    ShardState::Stopping => {
                        // shard already stopping
                    }
                }
            }
            Entry::Vacant(_) => {}
        }
    }
}

impl ShardHost {
    fn stop_shard(
        &self,
        shard_id: ShardId,
        actor_ref: LocalActorRef<Shard>,
        ctx: &ActorContext,
        stop_requested: Option<StopRequested>,
    ) {
        let shard_host = self.actor_ref(ctx);
        let remote_system = ctx.system().remote_owned();
        tokio::spawn(async move {
            let result = actor_ref.stop().await;
            let _ = shard_host.notify(ShardStopped {
                shard_id,
                stopped_successfully: result.is_ok(),
            });

            if let Some(stop_requested) = stop_requested {
                let shard_stopped = ShardStopped {
                    shard_id,
                    stopped_successfully: result.is_ok(),
                }
                .into_envelope(EnvelopeType::Remote)
                .unwrap()
                .into_bytes();

                let _ = remote_system
                    .notify_raw_rpc_result(
                        stop_requested.request_id,
                        shard_stopped,
                        stop_requested.origin_node_id,
                    )
                    .await;
            }
        });
    }
}

#[async_trait]
impl Handler<ShardStopped> for ShardHost {
    async fn handle(&mut self, message: ShardStopped, _ctx: &mut ActorContext) {
        self.hosted_shards.remove(&message.shard_id);
        info!("shard#{} stopped", message.shard_id);
    }
}

pub fn shard_actor_id(shard_entity: &String, shard_id: ShardId) -> ActorId {
    format!("{}-Shard-{}", &shard_entity, shard_id).into_actor_id()
}

impl Message for ShardAllocated {
    type Result = ();

    fn as_bytes(&self) -> Result<Vec<u8>, MessageWrapErr> {
        proto::ShardAllocated {
            shard: Some(proto::RemoteShard {
                shard_id: self.0,
                node_id: self.1,
                ..Default::default()
            })
            .into(),
            ..Default::default()
        }
        .write_to_bytes()
        .map_err(|_| MessageWrapErr::SerializationErr)
    }

    fn from_bytes(b: Vec<u8>) -> Result<Self, MessageUnwrapErr> {
        proto::ShardAllocated::parse_from_bytes(&b)
            .map(|r| {
                let shard = r.shard.unwrap();
                Self(shard.shard_id, shard.node_id)
            })
            .map_err(|_e| MessageUnwrapErr::DeserializationErr)
    }

    fn read_remote_result(_: Vec<u8>) -> Result<Self::Result, MessageUnwrapErr> {
        Ok(())
    }

    fn write_remote_result(_res: Self::Result) -> Result<Vec<u8>, MessageWrapErr> {
        Ok(vec![])
    }
}

impl Message for ShardReallocating {
    type Result = ();

    fn as_bytes(&self) -> Result<Vec<u8>, MessageWrapErr> {
        proto::ShardReallocating {
            shard_id: self.0,
            ..Default::default()
        }
        .write_to_bytes()
        .map_err(|_| MessageWrapErr::SerializationErr)
    }

    fn from_bytes(b: Vec<u8>) -> Result<Self, MessageUnwrapErr> {
        proto::StopShard::parse_from_bytes(&b)
            .map(|r| Self(r.shard_id))
            .map_err(|_e| MessageUnwrapErr::DeserializationErr)
    }

    fn read_remote_result(_: Vec<u8>) -> Result<Self::Result, MessageUnwrapErr> {
        Ok(())
    }

    fn write_remote_result(_res: Self::Result) -> Result<Vec<u8>, MessageWrapErr> {
        Ok(vec![])
    }
}

impl Message for StopShard {
    type Result = ();

    fn as_bytes(&self) -> Result<Vec<u8>, MessageWrapErr> {
        proto::StopShard {
            shard_id: self.shard_id,
            request_id: self.request_id.to_string(),
            origin_node_id: self.origin_node_id,
            ..Default::default()
        }
        .write_to_bytes()
        .map_err(|_| MessageWrapErr::SerializationErr)
    }

    fn from_bytes(b: Vec<u8>) -> Result<Self, MessageUnwrapErr> {
        proto::StopShard::parse_from_bytes(&b)
            .map(|r| Self {
                shard_id: r.shard_id,
                request_id: Uuid::parse_str(&r.request_id).unwrap(),
                origin_node_id: r.origin_node_id,
            })
            .map_err(|_e| MessageUnwrapErr::DeserializationErr)
    }

    fn read_remote_result(_: Vec<u8>) -> Result<Self::Result, MessageUnwrapErr> {
        Ok(())
    }

    fn write_remote_result(_res: Self::Result) -> Result<Vec<u8>, MessageWrapErr> {
        Ok(vec![])
    }
}

impl Message for ShardStopped {
    type Result = ();

    fn as_bytes(&self) -> Result<Vec<u8>, MessageWrapErr> {
        proto::ShardStopped {
            shard_id: self.shard_id,
            is_successful: self.stopped_successfully,
            ..Default::default()
        }
        .write_to_bytes()
        .map_err(|_| MessageWrapErr::SerializationErr)
    }

    fn from_bytes(b: Vec<u8>) -> Result<Self, MessageUnwrapErr> {
        proto::ShardStopped::parse_from_bytes(&b)
            .map(|r| Self {
                shard_id: r.shard_id,
                stopped_successfully: r.is_successful,
            })
            .map_err(|_e| MessageUnwrapErr::DeserializationErr)
    }

    fn read_remote_result(_: Vec<u8>) -> Result<Self::Result, MessageUnwrapErr> {
        Ok(())
    }

    fn write_remote_result(_res: Self::Result) -> Result<Vec<u8>, MessageWrapErr> {
        Ok(vec![])
    }
}

impl Message for GetShards {
    type Result = HostedShards;
}
