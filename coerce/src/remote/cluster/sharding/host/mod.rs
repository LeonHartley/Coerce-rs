use crate::actor::context::{ActorContext, ActorStatus};
use crate::actor::message::{
    Envelope, EnvelopeType, Handler, Message, MessageUnwrapErr, MessageWrapErr,
};
use crate::actor::{Actor, ActorId, ActorRef, ActorRefErr, IntoActor, LocalActorRef};
use crate::remote::cluster::sharding::coordinator::ShardId;
use crate::remote::cluster::sharding::host::request::EntityRequest;
use crate::remote::cluster::sharding::proto::sharding::ShardAllocated as ShardAllocatedProto;
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
    max_shards: ShardId,
    hosted_shards: HashMap<ShardId, ShardState>,
    remote_shards: HashMap<ShardId, ActorRef<Shard>>,
    buffered_requests: HashMap<ShardId, Vec<EntityRequest>>,
}

impl Actor for ShardHost {}

impl ShardHost {
    pub fn new(shard_entity: String) -> ShardHost {
        ShardHost {
            shard_entity,
            max_shards: 100,
            hosted_shards: Default::default(),
            remote_shards: Default::default(),
            buffered_requests: Default::default(),
        }
    }
}

pub struct ShardAllocated(pub ShardId, pub NodeId);

pub struct StopShard {
    shard_id: ShardId,
}

pub struct StartEntity {
    pub actor_id: ActorId,
    pub recipe: Vec<u8>,
}

#[async_trait]
impl Handler<ShardAllocated> for ShardHost {
    async fn handle(&mut self, message: ShardAllocated, ctx: &mut ActorContext) {
        let remote = ctx.system().remote();

        let shard_id = message.0;
        let node_id = message.1;
        let shard_actor_id = shard_actor_id(&self.shard_entity, shard_id);

        if node_id == remote.node_id() && !ctx.boxed_child_ref(&shard_actor_id).is_some() {
            let handler = remote
                .config()
                .actor_handler(&self.shard_entity)
                .expect("actor factory not supported");

            let shard = Shard::new(shard_id, handler)
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

// TODO: Allow this to be overridden
pub fn calculate_shard_id(actor_id: &ActorId, max_shards: ShardId) -> ShardId {
    let hashed_actor_id = {
        let mut hasher = DefaultHasher::new();
        actor_id.hash(&mut hasher);
        hasher.finish()
    };

    (hashed_actor_id % max_shards as u64) as ShardId
}

pub fn shard_actor_id(shard_entity: &String, shard_id: ShardId) -> ActorId {
    format!("{}-Shard#{}", &shard_entity, shard_id)
}

impl Message for ShardAllocated {
    type Result = ();

    fn as_remote_envelope(&self) -> Result<Envelope<Self>, MessageWrapErr> {
        ShardAllocatedProto {
            shard_id: self.0,
            node_id: self.1,
            ..Default::default()
        }
        .write_to_bytes()
        .map_or_else(
            |_e| Err(MessageWrapErr::SerializationErr),
            |m| Ok(Envelope::Remote(m)),
        )
    }

    fn from_remote_envelope(b: Vec<u8>) -> Result<Self, MessageUnwrapErr> {
        ShardAllocatedProto::parse_from_bytes(&b)
            .map(|r| Self(r.shard_id, r.node_id))
            .map_err(|e| MessageUnwrapErr::DeserializationErr)
    }

    fn write_remote_result(_res: Self::Result) -> Result<Vec<u8>, MessageWrapErr> {
        Ok(vec![])
    }

    fn read_remote_result(_: Vec<u8>) -> Result<Self::Result, MessageUnwrapErr> {
        Ok(())
    }
}

impl Message for StopShard {
    type Result = ();
}

impl Message for StartEntity {
    type Result = ();
}
