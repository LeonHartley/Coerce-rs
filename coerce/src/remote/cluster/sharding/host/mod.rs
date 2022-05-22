use crate::actor::context::ActorContext;
use crate::actor::message::{Envelope, Handler, Message, MessageUnwrapErr, MessageWrapErr};
use crate::actor::{
    Actor, ActorId, ActorRef, ActorRefErr, BoxedActorRef, IntoActor, LocalActorRef,
};
use crate::remote::cluster::sharding::coordinator::allocation::DefaultAllocator;
use crate::remote::cluster::sharding::coordinator::{ShardCoordinator, ShardId};
use crate::remote::cluster::sharding::host::request::{handle_request, EntityRequest};
use crate::remote::cluster::sharding::proto::sharding as proto;

use crate::remote::cluster::sharding::shard::Shard;
use crate::remote::system::NodeId;
use crate::remote::RemoteActorRef;
use protobuf::Message as ProtoMessage;

use crate::remote::actor::BoxedActorHandler;
use futures::FutureExt;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use uuid::Uuid;

pub mod request;
pub mod stats;

pub enum ShardState {
    Starting {
        request_buffer: Vec<EntityRequest>,
        stop_requested: bool,
    },
    Ready(LocalActorRef<Shard>),
    Stopping,
}

pub struct ShardHost {
    shard_entity: String,
    actor_handler: BoxedActorHandler,
    hosted_shards: HashMap<ShardId, ShardState>,
    remote_shards: HashMap<ShardId, ActorRef<Shard>>,
    requests_pending_leader_allocation: VecDeque<EntityRequest>,
    requests_pending_shard_allocation: HashMap<ShardId, Vec<EntityRequest>>,
    allocator: Box<dyn ShardAllocator + Send + Sync>,
}

pub trait ShardAllocator {
    fn allocate(&mut self, actor_id: &ActorId) -> ShardId;
}

pub struct LeaderAllocated;

impl Message for LeaderAllocated {
    type Result = ();
}

impl Actor for ShardHost {}

impl ShardHost {
    pub fn new(
        shard_entity: String,
        actor_handler: BoxedActorHandler,
        allocator: Option<Box<dyn ShardAllocator + Send + Sync>>,
    ) -> ShardHost {
        ShardHost {
            shard_entity,
            actor_handler,
            hosted_shards: Default::default(),
            remote_shards: Default::default(),
            requests_pending_shard_allocation: Default::default(),
            requests_pending_leader_allocation: Default::default(),
            allocator: allocator.map_or_else(
                || Box::new(DefaultAllocator::default()) as Box<dyn ShardAllocator + Send + Sync>,
                |s| s,
            ),
        }
    }

    pub async fn get_coordinator(&self, ctx: &ActorContext) -> ActorRef<ShardCoordinator> {
        let actor_id = format!("ShardCoordinator-{}", &self.shard_entity);
        let remote = ctx.system().remote();
        let leader = remote.current_leader();
        if leader == Some(remote.node_id()) {
            ctx.system()
                .get_tracked_actor::<ShardCoordinator>(actor_id)
                .await
                .expect("get local coordinator")
                .into()
        } else {
            RemoteActorRef::<ShardCoordinator>::new(actor_id, leader.unwrap(), remote.clone())
                .into()
        }
    }
}

pub struct GetCoordinator;

impl Message for GetCoordinator {
    type Result = ActorRef<ShardCoordinator>;
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

impl Message for ShardStopped {
    type Result = ();
}

pub struct StopShard {
    pub shard_id: ShardId,
    pub request_id: Uuid,
}

pub struct StartEntity {
    pub actor_id: ActorId,
    pub recipe: Vec<u8>,
}

pub struct RemoveEntity {
    pub actor_id: ActorId,
}

pub struct PassivateEntity {
    pub actor_id: String,
}

impl ShardState {
    pub fn actor_ref(&self) -> Option<LocalActorRef<Shard>> {
        match self {
            ShardState::Ready(actor_ref) => Some(actor_ref.clone()),
            _ => None,
        }
    }
}

#[async_trait]
impl Handler<GetCoordinator> for ShardHost {
    async fn handle(
        &mut self,
        _message: GetCoordinator,
        ctx: &mut ActorContext,
    ) -> ActorRef<ShardCoordinator> {
        self.get_coordinator(&ctx).await
    }
}

#[async_trait]
impl Handler<LeaderAllocated> for ShardHost {
    async fn handle(&mut self, _message: LeaderAllocated, ctx: &mut ActorContext) {
        if self.requests_pending_leader_allocation.len() > 0 {
            debug!(
                "processing {} buffered requests_pending_leader_allocation",
                self.requests_pending_leader_allocation.len(),
            );

            while let Some(request) = self.requests_pending_leader_allocation.pop_front() {
                self.handle(request, ctx).await;
            }
        }
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
            if self.remote_shards.contains_key(&shard_id) {
                let _ = self.remote_shards.remove(&shard_id);
            }

            let self_ref = self.actor_ref(ctx);
            let handler = self.actor_handler.new_boxed();

            let hosted_shard_entry = self.hosted_shards.entry(shard_id);
            match hosted_shard_entry {
                Entry::Occupied(_) => {
                    // already allocated locally
                }

                Entry::Vacant(hosted_shard_entry) => {
                    let system = ctx.system().clone();

                    tokio::spawn(async move {
                        let shard = Shard::new(shard_id, handler, true)
                            .into_actor(Some(shard_actor_id), &system)
                            .await
                            .expect("create shard actor");

                        self_ref.notify(ShardReady(shard_id, shard))
                    });

                    hosted_shard_entry.insert(ShardState::Starting {
                        request_buffer: vec![],
                        stop_requested: false,
                    });

                    debug!("local shard#{} allocated to node={}", message.0, message.1);
                }
            }
        } else {
            // if self.hosted_shards.contains_key(&shard_id) {
            //     // log an error or a warning?
            // }

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
            }) => {
                if stop_requested {
                    // TODO: move the request buffer over to `requests_pending_shard_allocation`
                    self.stop_shard(shard_id, actor_ref, ctx);
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
        match shard_entry {
            Entry::Occupied(mut shard_entry) => {
                match shard_entry.insert(ShardState::Stopping) {
                    ShardState::Starting {
                        mut stop_requested, ..
                    } => {
                        // actor has requested to be started so we can't stop it yet, once the actor
                        // has finished starting, we can then stop it.
                        stop_requested = true;
                    }

                    ShardState::Ready(actor_ref) => self.stop_shard(shard_id, actor_ref, ctx),

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
    fn stop_shard(&self, shard_id: ShardId, actor_ref: LocalActorRef<Shard>, ctx: &ActorContext) {
        let shard_host = self.actor_ref(ctx);

        tokio::spawn(async move {
            let result = actor_ref.stop().await;
            let _ = shard_host.notify(ShardStopped {
                shard_id,
                stopped_successfully: result.is_ok(),
            });

            // TODO: send `ShardStopped` result back to the sending node (if it was remote)
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
    format!("{}-Shard-{}", &shard_entity, shard_id)
}

impl Message for ShardAllocated {
    type Result = ();

    fn as_remote_envelope(&self) -> Result<Envelope<Self>, MessageWrapErr> {
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
        .map_or_else(
            |_e| Err(MessageWrapErr::SerializationErr),
            |m| Ok(Envelope::Remote(m)),
        )
    }

    fn from_remote_envelope(b: Vec<u8>) -> Result<Self, MessageUnwrapErr> {
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

    fn as_remote_envelope(&self) -> Result<Envelope<Self>, MessageWrapErr> {
        proto::ShardReallocating {
            shard_id: self.0,
            ..Default::default()
        }
        .write_to_bytes()
        .map_or_else(
            |_e| Err(MessageWrapErr::SerializationErr),
            |m| Ok(Envelope::Remote(m)),
        )
    }

    fn from_remote_envelope(b: Vec<u8>) -> Result<Self, MessageUnwrapErr> {
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

    fn as_remote_envelope(&self) -> Result<Envelope<Self>, MessageWrapErr> {
        proto::StopShard {
            shard_id: self.shard_id,
            ..Default::default()
        }
        .write_to_bytes()
        .map_or_else(
            |_e| Err(MessageWrapErr::SerializationErr),
            |m| Ok(Envelope::Remote(m)),
        )
    }

    fn from_remote_envelope(b: Vec<u8>) -> Result<Self, MessageUnwrapErr> {
        proto::StopShard::parse_from_bytes(&b)
            .map(|r| Self {
                shard_id: r.shard_id,
                request_id: Uuid::new_v4(),
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

impl Message for StartEntity {
    type Result = ();

    fn as_remote_envelope(&self) -> Result<Envelope<Self>, MessageWrapErr> {
        proto::StartEntity {
            actor_id: self.actor_id.clone(),
            recipe: self.recipe.clone(),
            ..Default::default()
        }
        .write_to_bytes()
        .map_or_else(
            |_e| Err(MessageWrapErr::SerializationErr),
            |m| Ok(Envelope::Remote(m)),
        )
    }

    fn from_remote_envelope(b: Vec<u8>) -> Result<Self, MessageUnwrapErr> {
        proto::StartEntity::parse_from_bytes(&b)
            .map(|r| Self {
                actor_id: r.actor_id,
                recipe: r.recipe,
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

impl Message for RemoveEntity {
    type Result = ();

    fn as_remote_envelope(&self) -> Result<Envelope<Self>, MessageWrapErr> {
        proto::RemoveEntity {
            actor_id: self.actor_id.clone(),
            ..Default::default()
        }
        .write_to_bytes()
        .map_or_else(
            |_e| Err(MessageWrapErr::SerializationErr),
            |m| Ok(Envelope::Remote(m)),
        )
    }

    fn from_remote_envelope(b: Vec<u8>) -> Result<Self, MessageUnwrapErr> {
        proto::RemoveEntity::parse_from_bytes(&b)
            .map(|r| Self {
                actor_id: r.actor_id,
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

impl Message for PassivateEntity {
    type Result = ();

    fn as_remote_envelope(&self) -> Result<Envelope<Self>, MessageWrapErr> {
        proto::PassivateEntity {
            actor_id: self.actor_id.clone(),
            ..Default::default()
        }
        .write_to_bytes()
        .map_or_else(
            |_e| Err(MessageWrapErr::SerializationErr),
            |m| Ok(Envelope::Remote(m)),
        )
    }

    fn from_remote_envelope(b: Vec<u8>) -> Result<Self, MessageUnwrapErr> {
        proto::PassivateEntity::parse_from_bytes(&b)
            .map(|r| Self {
                actor_id: r.actor_id,
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
