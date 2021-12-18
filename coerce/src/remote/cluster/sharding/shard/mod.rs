use crate::actor::context::{ActorContext, ActorStatus};
use crate::actor::message::{Handler, Message};
use crate::actor::{Actor, ActorId, ActorRefErr, BoxedActorRef};
use crate::remote::actor::BoxedActorHandler;
use crate::remote::cluster::sharding::coordinator::ShardId;
use crate::remote::cluster::sharding::host::StartEntity;
use crate::remote::handler::ActorHandler;
use crate::remote::net::message::SessionEvent;
use crate::remote::net::proto::network::ClientResult;

use crate::actor::system::ActorSystem;
use crate::remote::cluster::sharding::host::request::{EntityRequest, RemoteEntityRequest};
use tokio::sync::oneshot;

pub struct Shard {
    shard_id: ShardId,
    handler: BoxedActorHandler,
    persistent_entities: bool,
}

impl Shard {
    pub fn new(shard_id: ShardId, handler: BoxedActorHandler) -> Shard {
        let persistent_entities = false;
        Shard {
            shard_id,
            handler,
            persistent_entities,
        }
    }
}

#[async_trait]
impl Actor for Shard {
    async fn started(&mut self, _ctx: &mut ActorContext) {
        debug!("started shard#{}", self.shard_id);
    }
}

impl Shard {
    async fn start_entity(
        &self,
        actor_id: ActorId,
        recipe: &Vec<u8>,
        ctx: &mut ActorContext,
    ) -> Result<BoxedActorRef, ActorRefErr> {
        let entity = self
            .handler
            .create(Some(actor_id), recipe.clone(), Some(ctx))
            .await;

        if self.persistent_entities && entity.is_ok() {
            // TODO: persist it
        }

        entity.map(|entity| {
            debug!(
                "spawned entity, registered it as a child of Shard#{}, hosted_entities={}",
                self.shard_id,
                &ctx.supervised_mut().unwrap().children.len()
            );
            ctx.attach_child_ref(entity.clone());
            entity
        })
    }
}

#[async_trait]
impl Handler<StartEntity> for Shard {
    async fn handle(&mut self, message: StartEntity, ctx: &mut ActorContext) {
        /*  TODO: `start_entity` should be done asynchronously, any messages sent while the actor is
                   starting should be buffered and emitted once the Shard receives confirmation that the actor was created
        */

        let _res = self
            .start_entity(message.actor_id, message.recipe.as_ref(), ctx)
            .await;
    }
}

#[async_trait]
impl Handler<EntityRequest> for Shard {
    async fn handle(&mut self, message: EntityRequest, ctx: &mut ActorContext) {
        let system = ctx.system().remote();
        let actor = ctx.boxed_child_ref(&message.actor_id);
        let handler = system.config().message_handler(&message.message_type);

        let actor_id = message.actor_id;
        let result_channel = message.result_channel;

        debug!(
            "entity request, node={}, actor_id={}, shard_id={}",
            system.node_id(),
            &actor_id,
            self.shard_id
        );
        if handler.is_none() {
            // TODO: send unsupported msg err
            warn!(
                "no message handler configured for message={}, actor_type={}, actor_id={}",
                message.message_type,
                self.handler.actor_type_name(),
                &actor_id
            );
            result_channel.map(|m| m.send(Err(ActorRefErr::ActorUnavailable)));
            return;
        }

        let handler = handler.unwrap();
        let actor = match actor {
            Some(actor) => actor,
            None => match message.recipe {
                /*  TODO: `start_entity` should be done asynchronously, any messages sent while the actor is
                          starting should be buffered and emitted once the Shard receives confirmation that the actor was created
                */
                Some(recipe) => match self.start_entity(actor_id, recipe.as_ref(), ctx).await {
                    Ok(actor) => actor,
                    Err(_err) => {
                        // TODO: Send actor could not be created err
                        result_channel.map(|m| m.send(Err(ActorRefErr::ActorUnavailable)));
                        return;
                    }
                },
                None => {
                    // TODO: send actor doesnt exist (and cannot be created) err
                    result_channel.map(|m| m.send(Err(ActorRefErr::ActorUnavailable)));
                    return;
                }
            },
        };

        let message = message.message;
        tokio::spawn(async move {
            handler
                .handle_direct(&actor, &message, result_channel)
                .await;
        });
    }
}

#[async_trait]
impl Handler<RemoteEntityRequest> for Shard {
    async fn handle(&mut self, message: RemoteEntityRequest, ctx: &mut ActorContext) {
        let (tx, rx) = oneshot::channel();

        debug!(
            "remote entity request, node={}, actor_id={}, shard_id={}, origin_node={}",
            ctx.system().remote().node_id(),
            &message.actor_id,
            self.shard_id,
            message.origin_node,
        );

        let origin_node = message.origin_node;
        let request_id = message.request_id;
        self.handle(
            {
                let mut message: EntityRequest = message.into();
                message.result_channel = Some(tx);
                message
            },
            ctx,
        )
        .await;

        let system = ctx.system().remote_owned();
        tokio::spawn(async move {
            match rx.await {
                Ok(bytes) => match bytes {
                    Ok(result) => {
                        let message_id = request_id.to_string();
                        let result = SessionEvent::Result(ClientResult {
                            message_id,
                            result,
                            ..Default::default()
                        });

                        system.node_rpc_raw(request_id, result, origin_node).await;
                    }
                    _ => {}
                },
                Err(_) => {}
            };
        });
    }
}
