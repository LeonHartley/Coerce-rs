use crate::actor::context::ActorContext;
use crate::actor::message::{
    Envelope, EnvelopeType, Handler, Message, MessageUnwrapErr, MessageWrapErr,
};
use crate::actor::{ActorId, ActorRef, ActorRefErr};
use crate::remote::cluster::sharding::coordinator::allocation::{
    AllocateShard, AllocateShardResult,
};
use crate::remote::cluster::sharding::coordinator::ShardCoordinator;
use crate::remote::cluster::sharding::host::{calculate_shard_id, ShardAllocated, ShardHost};
use crate::remote::cluster::sharding::proto::sharding::{
    AllocateShard as AllocateShardProto, RemoteEntityRequest as RemoteEntityRequestProto,
    RemoteEntityRequest_Recipe, ShardAllocated as ShardAllocatedProto,
};
use crate::remote::cluster::sharding::shard::Shard;
use crate::remote::system::{NodeId, RemoteActorSystem};
use crate::remote::RemoteActorRef;
use bytes::Bytes;
use protobuf::{Message as ProtoMessage, SingularPtrField};
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
            let remote = ctx.system().remote();
            let leader = remote.current_leader();

            if let Some(leader) = leader {
                let actor_id = format!("ShardCoordinator-{}", &self.shard_entity);

                let leader: ActorRef<ShardCoordinator> = if leader == remote.node_id() {
                    // TODO: Cache the coordinator ref
                    ctx.system()
                        .get_tracked_actor::<ShardCoordinator>(actor_id)
                        .await
                        .expect("get local coordinator")
                        .into()
                } else {
                    RemoteActorRef::<ShardCoordinator>::new(actor_id, leader, remote.clone()).into()
                };

                let buffered_requests = self.buffered_requests.entry(shard_id);
                let mut buffered_requests = buffered_requests.or_insert_with(|| vec![]);
                buffered_requests.push(message);

                debug!("shard#{} not allocated, notifying coordinator and buffering request (buffered_requests={})", shard_id, buffered_requests.len());

                let host_ref = ctx.actor_ref::<Self>();
                tokio::spawn(async move {
                    let allocation = leader.send(AllocateShard { shard_id }).await;
                    if let Ok(AllocateShardResult::Allocated(node_id)) = allocation {
                        host_ref.notify(ShardAllocated(shard_id, node_id));
                    }
                });
            }
        }
    }
}

async fn remote_entity_request(
    shard_ref: ActorRef<Shard>,
    mut request: EntityRequest,
    system: RemoteActorSystem,
) -> Result<(), ActorRefErr> {
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
            actor_id: request.actor_id,
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
                let response = response
                    .into_result()
                    .map_err(|_| ActorRefErr::ActorUnavailable);

                result_sender.send(response)
            });

            Ok(())
        }
        Err(_) => Err(ActorRefErr::ActorUnavailable),
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
            actor_id: req.actor_id,
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

    fn as_remote_envelope(&self) -> Result<Envelope<Self>, MessageWrapErr> {
        let proto = RemoteEntityRequestProto {
            request_id: self.request_id.to_string(),
            actor_id: self.actor_id.clone(),
            message_type: self.message_type.clone(),
            message: self.message.clone(),
            recipe: self.recipe.clone().map_or_else(
                || SingularPtrField::none(),
                |r| {
                    SingularPtrField::some(RemoteEntityRequest_Recipe {
                        recipe: r,
                        ..Default::default()
                    })
                },
            ),
            origin_node: self.origin_node,
            ..Default::default()
        };

        proto.write_to_bytes().map_or_else(
            |e| Err(MessageWrapErr::SerializationErr),
            |bytes| Ok(Envelope::Remote(bytes)),
        )
    }

    fn from_remote_envelope(buffer: Vec<u8>) -> Result<Self, MessageUnwrapErr> {
        RemoteEntityRequestProto::parse_from_bytes(&buffer).map_or_else(
            |_e| Err(MessageUnwrapErr::DeserializationErr),
            |proto| {
                Ok(RemoteEntityRequest {
                    request_id: Uuid::from_str(&proto.request_id).unwrap(),
                    actor_id: proto.actor_id,
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
