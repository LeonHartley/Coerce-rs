use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message, MessageUnwrapErr, MessageWrapErr};
use crate::actor::{Actor, ActorId, ActorRef, ActorRefErr, IntoActorId};
use crate::sharding::coordinator::allocation::{AllocateShard, AllocateShardResult};

use crate::remote::system::{NodeId, RemoteActorSystem};
use crate::sharding::host::{ShardAllocated, ShardHost, ShardState};
use crate::sharding::proto::sharding as proto;
use crate::sharding::shard::Shard;

use crate::sharding::coordinator::ShardId;
use protobuf::Message as ProtoMessage;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::oneshot::{channel, Sender};
use uuid::Uuid;

pub struct EntityRequest {
    pub actor_id: ActorId,
    pub message_type: String,
    pub message: Vec<u8>,
    pub recipe: Option<Arc<Vec<u8>>>,
    pub result_channel: Option<Sender<Result<Vec<u8>, ActorRefErr>>>,
}

pub struct RemoteEntityRequest {
    pub request_id: Uuid,
    pub actor_id: ActorId,
    pub message_type: String,
    pub message: Vec<u8>,
    pub recipe: Option<Vec<u8>>,
    pub origin_node: NodeId,
}

pub(crate) fn handle_request(
    message: EntityRequest,
    shard_id: ShardId,
    shard_state: &mut ShardState,
) {
    match shard_state {
        ShardState::Starting { request_buffer, .. } => request_buffer.push(message),

        ShardState::Ready(actor) => {
            let actor = actor.clone();
            tokio::spawn(async move {
                let actor_id = message.actor_id.clone();
                let message_type = message.message_type.clone();

                let result = actor.send(message).await;
                if !result.is_ok() {
                    error!(
                        "failed to deliver EntityRequest (actor_id={}, type={}) to shard (shard_id={})",
                        &actor_id, &message_type, shard_id,
                    );
                } else {
                    trace!(
                        "delivered EntityRequest (actor_id={}, type={}) to shard (shard_id={})",
                        &actor_id,
                        message_type,
                        shard_id,
                    );
                }
            });
        }
        ShardState::Stopping => {}
    }
}

#[async_trait]
impl Handler<EntityRequest> for ShardHost {
    async fn handle(&mut self, message: EntityRequest, ctx: &mut ActorContext) {
        let shard_id = self.allocator.allocate(&message.actor_id);

        if let Some(shard) = self.hosted_shards.get_mut(&shard_id) {
            handle_request(message, shard_id, shard);
        } else if let Some(shard) = self.remote_shards.get(&shard_id) {
            let shard_ref = shard.clone();
            tokio::spawn(remote_entity_request(
                shard_ref,
                message,
                ctx.system().remote_owned(),
            ));
        } else {
            let leader = self.get_coordinator();

            let buffered_requests = self.requests_pending_shard_allocation.entry(shard_id);
            let buffered_requests = buffered_requests.or_insert_with(|| vec![]);
            buffered_requests.push(message);

            debug!("shard#{} not allocated, notifying coordinator and buffering request (buffered_requests={})",
                shard_id, buffered_requests.len());

            let host_ref = self.actor_ref(ctx);

            tokio::spawn(async move {
                let allocation = leader
                    .send(AllocateShard {
                        shard_id,
                        rebalancing: false,
                    })
                    .await;
                match allocation {
                    Ok(result) => match result {
                        AllocateShardResult::Allocated(shard_id, node_id)
                        | AllocateShardResult::AlreadyAllocated(shard_id, node_id) => {
                            let _ = host_ref.notify(ShardAllocated(shard_id, node_id));
                        }
                        AllocateShardResult::NotAllocated => {
                            // No hosts available?
                            error!("shard(#{}) not allocated, no hosts available?", shard_id);
                        }
                        AllocateShardResult::Err(e) => {
                            error!("shard(#{}) failed to allocate: {}", shard_id, e);
                        }
                    },
                    Err(_e) => {}
                }
            });
        }
    }
}

async fn remote_entity_request(
    shard_ref: ActorRef<Shard>,
    mut request: EntityRequest,
    system: RemoteActorSystem,
) -> Result<(), ActorRefErr> {
    // TODO: We need a way to buffer and re-distribute this request if we get a signal that the node is terminated
    //       after we have already submitted the request..

    let request_id = Uuid::new_v4();
    let (tx, rx) = channel();
    system.push_request(request_id, tx);

    let result_channel = request.result_channel.take();

    trace!(
        "emitting RemoteEntityRequest to node_id={} from node_id={}",
        shard_ref.node_id().unwrap(),
        system.node_id()
    );

    shard_ref
        .notify(RemoteEntityRequest {
            origin_node: system.node_id(),
            request_id,
            actor_id: request.actor_id.into_actor_id(),
            message_type: request.message_type,
            message: request.message,
            recipe: request.recipe.map(|r| r.as_ref().clone()),
        })
        .await
        .expect("shard notify");

    trace!(
        "emitted RemoteEntityRequest to node_id={} from node_id={}, waiting for reply",
        shard_ref.node_id().unwrap(),
        system.node_id()
    );

    let res = match rx.await {
        Ok(response) => {
            result_channel.map(move |result_sender| {
                let response = response.into_result().map_err(|e| e);

                result_sender.send(response)
            });

            Ok(())
        }
        Err(_e) => Err(ActorRefErr::ResultChannelClosed),
    };

    trace!(
        "received reply for RemoteEntityRequest from node_id={} to node_id={}",
        system.node_id(),
        shard_ref.node_id().unwrap()
    );

    res
}

impl From<RemoteEntityRequest> for EntityRequest {
    fn from(req: RemoteEntityRequest) -> Self {
        EntityRequest {
            actor_id: req.actor_id.into_actor_id(),
            message_type: req.message_type,
            message: req.message,
            recipe: req.recipe.map(|r| Arc::new(r)),
            result_channel: None,
        }
    }
}

impl Message for EntityRequest {
    type Result = ();
}

impl Message for RemoteEntityRequest {
    type Result = ();

    fn as_bytes(&self) -> Result<Vec<u8>, MessageWrapErr> {
        proto::RemoteEntityRequest {
            request_id: self.request_id.to_string(),
            actor_id: self.actor_id.to_string(),
            message_type: self.message_type.clone(),
            message: self.message.clone(),
            recipe: self.recipe.clone().map_or_else(
                || None.into(),
                |r| {
                    Some(proto::remote_entity_request::Recipe {
                        recipe: r,
                        ..Default::default()
                    })
                    .into()
                },
            ),
            origin_node: self.origin_node,
            ..Default::default()
        }
        .write_to_bytes()
        .map_err(|_| MessageWrapErr::SerializationErr)
    }

    fn from_bytes(buffer: Vec<u8>) -> Result<Self, MessageUnwrapErr> {
        proto::RemoteEntityRequest::parse_from_bytes(&buffer).map_or_else(
            |_e| Err(MessageUnwrapErr::DeserializationErr),
            |proto| {
                Ok(RemoteEntityRequest {
                    request_id: Uuid::from_str(&proto.request_id).unwrap(),
                    actor_id: proto.actor_id.into_actor_id(),
                    message_type: proto.message_type,
                    message: proto.message,
                    recipe: proto
                        .recipe
                        .into_option()
                        .map_or(None, |recipe| Some(recipe.recipe)),
                    origin_node: proto.origin_node,
                })
            },
        )
    }

    fn read_remote_result(_buffer: Vec<u8>) -> Result<Self::Result, MessageUnwrapErr> {
        Ok(())
    }

    fn write_remote_result(_res: Self::Result) -> Result<Vec<u8>, MessageWrapErr> {
        Ok(vec![])
    }
}
