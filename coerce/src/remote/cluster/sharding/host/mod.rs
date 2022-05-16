use crate::actor::context::ActorContext;
use crate::actor::message::{Envelope, Handler, Message, MessageUnwrapErr, MessageWrapErr};
use crate::actor::{Actor, ActorId, ActorRef, IntoActor, LocalActorRef};
use crate::remote::cluster::sharding::coordinator::allocation::DefaultAllocator;
use crate::remote::cluster::sharding::coordinator::{ShardCoordinator, ShardId};
use crate::remote::cluster::sharding::host::request::EntityRequest;
use crate::remote::cluster::sharding::proto::sharding as proto;

use crate::remote::cluster::sharding::shard::Shard;
use crate::remote::system::NodeId;
use crate::remote::RemoteActorRef;
use protobuf::Message as ProtoMessage;

use crate::remote::actor::BoxedActorHandler;
use std::collections::{HashMap, VecDeque};

pub mod request;
pub mod stats;

pub enum ShardState {
    Starting { request_buffer: Vec<EntityRequest> },
    Ready(LocalActorRef<Shard>),
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

pub struct StopShard {
    pub shard_id: ShardId,
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

            if ctx.boxed_child_ref(&shard_actor_id).is_some() {
                return;
            }

            let system = ctx.system().clone();
            let handler = self.actor_handler.new_boxed();

            let shard = Shard::new(shard_id, handler, true)
                .into_actor(Some(shard_actor_id), &system)
                .await
                .expect("create shard actor");

            ctx.attach_child_ref(shard.clone().into());

            // let self_ref = self.actor_ref(ctx);
            // tokio::spawn(async move {
            //     let shard = Shard::new(shard_id, handler, true)
            //         .into_actor(Some(shard_actor_id), &system)
            //         .await
            //         .expect("create shard actor");
            //
            //     self_ref.notify(ShardStarted(shard_id, shard))
            // });
            // self.hosted_shards
            //     .insert(shard_id, ShardState::Starting { request_buffer: vec![] });

            debug!("local shard#{} allocated to node={}", message.0, message.1);
            self.hosted_shards
                .insert(shard_id, ShardState::Ready(shard));
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

impl Handler<ShardReady> for ShardHost {
    async fn handle(&mut self, message: ShardReady, ctx: &mut ActorContext) {
        let shard_state = self
            .hosted_shards
            .insert(message.0, ShardState::Ready(message.1.clone()));
        match shard_state {
            Some(ShardState::Starting { request_buffer }) => {
                for request in request_buffer {
                    self.handle_request(request, &mut ShardState::Ready(message.1.clone()))
                        .await;
                }
            }
            _ => {}
        }
        ctx.attach_child_ref(shard.clone().into());
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
    async fn handle(&mut self, message: StopShard, _ctx: &mut ActorContext) {
        // TODO: having the shard host wait for the shard to stop
        //       will hurt throughput of other shards during re-balancing,
        //       need a way to defer it and return a remote result via oneshot channel

        let status = match self.hosted_shards.remove(&message.shard_id) {
            None => Ok(()),
            Some(shard) => shard.actor.stop().await,
        };

        match status {
            Ok(_) => {}
            Err(_) => {}
        }
    }
}

pub fn shard_actor_id(shard_entity: &String, shard_id: ShardId) -> ActorId {
    format!("{}-Shard#{}", &shard_entity, shard_id)
}

impl Message for ShardReady {
    type Result = ();
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
