use crate::actor::context::ActorContext;
use crate::actor::message::Handler;
use crate::actor::Actor;
use crate::remote::actor::BoxedActorHandler;
use crate::remote::cluster::sharding::coordinator::ShardId;
use crate::remote::cluster::sharding::host::StartEntity;
use crate::remote::handler::ActorHandler;
use crate::remote::net::proto::protocol::CreateActor;
use tokio::sync::oneshot;

pub struct Shard {
    shard_id: ShardId,
    handler: BoxedActorHandler,
}

impl Actor for Shard {}

#[async_trait]
impl Handler<StartEntity> for Shard {
    async fn handle(&mut self, message: StartEntity, ctx: &mut ActorContext) {
        let system = ctx.system().remote();
        let created_actor = self.handler
            .create(
                Some(message.actor_id),
                message.recipe,
                Some(ctx),
            )
            .await;
    }
}
