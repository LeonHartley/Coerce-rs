use crate::actor::context::ActorContext;
use crate::actor::message::Handler;
use crate::actor::Actor;
use crate::remote::cluster::sharding::coordinator::ShardId;
use crate::remote::cluster::sharding::host::StartEntity;
use crate::remote::handler::ActorHandler;
use crate::remote::net::proto::protocol::CreateActor;

pub struct Shard {
    shard_id: ShardId,
    handler: Box<dyn ActorHandler>
}

impl Actor for Shard {

}

impl Handler<StartEntity> for Shard {
    async fn handle(&mut self, message: StartEntity, ctx: &mut ActorContext) {
        let create_args = CreateActor {
            message_id: "".to_string(),
            actor_id: "".to_string(),
            actor_type: "".to_string(),
            recipe: vec![],
            trace_id: "".to_string(),

        };
        let creation_result = self.handler.create(message.).await;
    }
}

