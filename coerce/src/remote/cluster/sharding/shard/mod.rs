use crate::actor::context::ActorContext;
use crate::actor::message::Handler;
use crate::actor::{Actor, ActorId, ActorRefErr, BoxedActorRef};
use crate::remote::actor::{BoxedActorHandler, BoxedMessageHandler};
use crate::remote::cluster::sharding::coordinator::ShardId;
use crate::remote::cluster::sharding::host::{EntityRequest, RemoteEntityRequest, StartEntity};
use crate::remote::handler::ActorHandler;
use crate::remote::net::message::SessionEvent;
use crate::remote::net::proto::protocol::{ClientResult, CreateActor};
use std::future::Future;
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::RecvError;

pub struct Shard {
    shard_id: ShardId,
    handler: BoxedActorHandler,
}

impl Shard {
    pub fn new(shard_id: ShardId, handler: BoxedActorHandler) -> Shard {
        Shard { shard_id, handler }
    }
}

impl Actor for Shard {}

impl Shard {
    async fn start_entity(
        &self,
        actor_id: ActorId,
        recipe: Vec<u8>,
        ctx: &mut ActorContext,
    ) -> Result<BoxedActorRef, ActorRefErr> {
        self.handler.create(Some(actor_id), recipe, Some(ctx)).await
    }
}

#[async_trait]
impl Handler<StartEntity> for Shard {
    async fn handle(&mut self, message: StartEntity, ctx: &mut ActorContext) {
        let res = self
            .start_entity(message.actor_id, message.recipe, ctx)
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

        if handler.is_none() {
            // TODO: send unsupported msg err
            result_channel.map(|m| m.send(Err(ActorRefErr::ActorUnavailable)));
            return;
        }

        let handler = handler.unwrap();
        let actor = match actor {
            Some(actor) => actor,
            None => match message.recipe {
                Some(recipe) => match self.start_entity(actor_id, recipe, ctx).await {
                    Ok(actor) => actor,
                    Err(err) => {
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

        let origin_node = message.origin_node;
        let request_id = message.request_id;
        self.handle(
            {
                let mut message = message.request;
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

                        system
                            .node_rpc_raw(request_id, result, origin_node)
                            .await;
                    }
                    _ => {}
                },
                Err(_) => {}
            };
        });
    }
}