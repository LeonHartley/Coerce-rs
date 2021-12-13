use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message};
use crate::actor::{ActorId, ActorRef, ActorRefErr};
use crate::remote::cluster::sharding::coordinator::allocation::AllocateShard;
use crate::remote::cluster::sharding::coordinator::ShardCoordinator;
use crate::remote::cluster::sharding::host::{calculate_shard_id, ShardHost};
use crate::remote::cluster::sharding::shard::Shard;
use crate::remote::system::{NodeId, RemoteActorSystem};
use crate::remote::RemoteActorRef;
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
    pub request: EntityRequest,
    pub origin_node: NodeId,
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
            let remote = ctx.system().remote();
            let leader = remote.current_leader();

            if let Some(leader) = leader {
                let actor_id = format!("ShardCoordinator-{}", &self.shard_entity);

                let leader: ActorRef<ShardCoordinator> = if leader == remote.node_id() {
                    ctx.system()
                        .get_tracked_actor::<ShardCoordinator>(actor_id)
                        .await
                        .expect("")
                        .into()
                } else {
                    RemoteActorRef::<ShardCoordinator>::new(actor_id, leader, remote.clone()).into()
                };

                let buffered_requests = self.buffered_requests.entry(shard_id);
                let mut buffered_requests = buffered_requests.or_insert_with(|| vec![]);
                buffered_requests.push(message);

                trace!("shard#{} not allocated, notifying coordinator and buffering request (buffered_requests={})", shard_id, buffered_requests.len());

                let _ = leader.notify(AllocateShard { shard_id }).await;
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
