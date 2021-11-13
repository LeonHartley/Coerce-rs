use crate::actor::context::{ActorContext, ActorStatus};
use crate::actor::message::{Handler, Message};
use crate::actor::{Actor, ActorId, ActorRef, ActorRefErr, IntoActor, LocalActorRef};
use crate::remote::cluster::sharding::coordinator::ShardId;
use crate::remote::cluster::sharding::shard::Shard;
use crate::remote::system::{NodeId, RemoteActorSystem};
use crate::remote::RemoteActorRef;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use tokio::sync::oneshot;

use tokio::sync::oneshot::Sender;
use uuid::Uuid;

struct ShardState {
    actor: LocalActorRef<Shard>,
}

pub struct ShardHost {
    shard_entity: String,
    max_shards: ShardId,
    hosted_shards: HashMap<ShardId, ShardState>,
    remote_shards: HashMap<ShardId, ActorRef<Shard>>,
}

impl Actor for ShardHost {}

pub struct ShardAllocated {
    shard_id: ShardId,
    node_id: NodeId,
}

pub struct StopShard {
    shard_id: ShardId,
}

pub struct StartEntity {
    pub actor_id: ActorId,
    pub recipe: Vec<u8>,
}

pub struct EntityRequest {
    pub actor_id: ActorId,
    pub message_type: String,
    pub message: Vec<u8>,
    pub recipe: Option<Vec<u8>>,
    pub result_channel: Option<Sender<Result<Vec<u8>, ActorRefErr>>>,
}

pub struct RemoteEntityRequest {
    pub request_id: Uuid,
    pub request: EntityRequest,
    pub origin_node: NodeId,
}

impl Message for ShardAllocated {
    type Result = ();
}

impl Message for StopShard {
    type Result = ();
}

impl Message for StartEntity {
    type Result = ();
}

impl Message for EntityRequest {
    type Result = ();
}

impl Message for RemoteEntityRequest {
    type Result = ();
}

#[async_trait]
impl Handler<EntityRequest> for ShardHost {
    async fn handle(&mut self, message: EntityRequest, ctx: &mut ActorContext) {
        let shard_id = calculate_shard_id(&message.actor_id, self.max_shards);

        if let Some(shard) = self.hosted_shards.get(&shard_id) {
            let actor = shard.actor.clone();
            tokio::spawn(async move {
                let actor_id = message.actor_id.clone();
                let message_type = message.message_type.clone();

                let result = actor.send(message).await;
                if !result.is_ok() {
                    error!(
                        "failed to deliver EntityRequest (actor_id={}, type={}) to shard (shard_id={})",
                        &actor_id, &message_type, shard_id
                    );
                } else {
                    trace!(
                        "delivered EntityRequest (actor_id={}, type={}) to shard (shard_id={})",
                        &actor_id,
                        message_type,
                        shard_id
                    );
                }
            });
        } else if let Some(shard) = self.remote_shards.get(&shard_id) {
            let shard_ref = shard.clone();
            tokio::spawn(remote_entity_request(
                shard_ref,
                message,
                ctx.system().remote_owned(),
            ));
        } else {
            // TODO: unallocated shard -> ask coordinator for the shard allocation
        }
    }
}

async fn remote_entity_request(
    shard_ref: ActorRef<Shard>,
    mut request: EntityRequest,
    system: RemoteActorSystem,
) -> Result<(), ActorRefErr> {
    let request_id = Uuid::new_v4();
    let (tx, rx) = oneshot::channel();
    system.push_request(request_id, tx).await;

    let result_channel = request.result_channel.take();

    shard_ref
        .notify(RemoteEntityRequest {
            origin_node: system.node_id(),
            request_id,
            request,
        })
        .await;

    match rx.await {
        Ok(response) => {
            result_channel.map(move |result_sender| {
                let response = response
                    .into_result()
                    .map_err(|_| ActorRefErr::ActorUnavailable);

                result_sender.send(response)
            });

            Ok(())
        }
        Err(_) => Err(ActorRefErr::ActorUnavailable),
    }
}

#[async_trait]
impl Handler<ShardAllocated> for ShardHost {
    async fn handle(&mut self, message: ShardAllocated, ctx: &mut ActorContext) {
        let remote = ctx.system().remote();
        if message.node_id == remote.node_id() {
            let handler = remote
                .config()
                .actor_handler(&self.shard_entity)
                .expect("actor factory not supported");

            let shard = Shard::new(message.shard_id, handler)
                .into_actor(
                    Some(shard_actor_id(&self.shard_entity, message.shard_id)),
                    ctx.system(),
                )
                .await
                .expect("create shard actor");

            self.hosted_shards
                .insert(message.shard_id, ShardState { actor: shard });
        } else {
            let shard_actor_id = shard_actor_id(&self.shard_entity, message.shard_id);
            let shard_actor =
                RemoteActorRef::new(shard_actor_id, message.node_id, ctx.system().remote_owned())
                    .into();

            self.remote_shards.insert(message.shard_id, shard_actor);
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
