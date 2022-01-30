use crate::actor::context::{ActorContext, ActorStatus};
use crate::actor::message::{
    Envelope, EnvelopeType, Handler, Message, MessageUnwrapErr, MessageWrapErr,
};
use crate::actor::{Actor, ActorId, ActorRef, ActorRefErr, IntoActor, LocalActorRef};
use crate::remote::cluster::sharding::coordinator::allocation::DefaultAllocator;
use crate::remote::cluster::sharding::coordinator::{ShardCoordinator, ShardId};
use crate::remote::cluster::sharding::host::request::EntityRequest;
use crate::remote::cluster::sharding::proto::sharding as proto;
use crate::remote::cluster::sharding::shard::stats::GetShardStats;
use crate::remote::cluster::sharding::shard::Shard;
use crate::remote::system::{NodeId, RemoteActorSystem};
use crate::remote::RemoteActorRef;
use protobuf::Message as ProtoMessage;
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, VecDeque};
use std::hash::{Hash, Hasher};
use tokio::sync::oneshot;
use tokio::sync::oneshot::Sender;
use uuid::Uuid;

pub mod request;
pub mod stats;

struct ShardState {
    actor: LocalActorRef<Shard>,
}

pub struct ShardHost {
    shard_entity: String,
    hosted_shards: HashMap<ShardId, ShardState>,
    remote_shards: HashMap<ShardId, ActorRef<Shard>>,
    buffered_requests: HashMap<ShardId, Vec<EntityRequest>>,
    allocator: Box<dyn ShardAllocator + Send + Sync>,
}

pub trait ShardAllocator {
    fn allocate(&mut self, actor_id: &ActorId) -> ShardId;
}

impl Actor for ShardHost {}

impl ShardHost {
    pub fn new(
        shard_entity: String,
        allocator: Option<Box<dyn ShardAllocator + Send + Sync>>,
    ) -> ShardHost {
        ShardHost {
            shard_entity,
            hosted_shards: Default::default(),
            remote_shards: Default::default(),
            buffered_requests: Default::default(),
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

pub struct StopShard {
    shard_id: ShardId,
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
impl Handler<ShardAllocated> for ShardHost {
    async fn handle(&mut self, message: ShardAllocated, ctx: &mut ActorContext) {
        let remote = ctx.system().remote();

        let shard_id = message.0;
        let node_id = message.1;
        let shard_actor_id = shard_actor_id(&self.shard_entity, shard_id);

        if node_id == remote.node_id() {
            if ctx.boxed_child_ref(&shard_actor_id).is_some() {
                return;
            }

            let handler = remote
                .config()
                .actor_handler(&self.shard_entity)
                .expect("actor factory not supported");

            let shard = Shard::new(shard_id, handler, true)
                .into_actor(Some(shard_actor_id), ctx.system())
                .await
                .expect("create shard actor");

            ctx.attach_child_ref(shard.clone().into());
            debug!("local shard#{} allocated to node={}", message.0, message.1);
            self.hosted_shards
                .insert(shard_id, ShardState { actor: shard });
        } else {
            let shard_actor =
                RemoteActorRef::new(shard_actor_id, node_id, ctx.system().remote_owned()).into();

            debug!("remote shard#{} allocated on node={}", message.0, message.1);
            self.remote_shards.insert(shard_id, shard_actor);
        }

        if let Some(buffered_requests) = self.buffered_requests.remove(&shard_id) {
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
impl Handler<StopShard> for ShardHost {
    async fn handle(&mut self, message: StopShard, _ctx: &mut ActorContext) {
        // TODO: having the shard host wait for the shard to stop
        //       will hurt throughput of other shards during re-balancing,
        //       need a way to defer it and return a remote result via oneshot channel

        let status = match self.hosted_shards.remove(&message.shard_id) {
            None => Ok(ActorStatus::Stopped),
            Some(shard) => shard.actor.stop().await,
        };

        match status {
            Ok(status) => match status {
                ActorStatus::Stopped => {}
                _ => {}
            },
            Err(_) => {}
        }
    }
}

pub fn shard_actor_id(shard_entity: &String, shard_id: ShardId) -> ActorId {
    format!("{}-Shard#{}", &shard_entity, shard_id)
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
            .map_err(|e| MessageUnwrapErr::DeserializationErr)
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
            .map_err(|e| MessageUnwrapErr::DeserializationErr)
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
            .map_err(|e| MessageUnwrapErr::DeserializationErr)
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
            .map_err(|e| MessageUnwrapErr::DeserializationErr)
    }

    fn read_remote_result(_: Vec<u8>) -> Result<Self::Result, MessageUnwrapErr> {
        Ok(())
    }

    fn write_remote_result(_res: Self::Result) -> Result<Vec<u8>, MessageWrapErr> {
        Ok(vec![])
    }
}
