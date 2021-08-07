use coerce::actor::context::ActorContext;
use coerce::actor::message::encoding::json::JsonMessage;
use coerce::actor::message::{Handler, Message};
use coerce::actor::Actor;

pub struct EchoActor;

impl Actor for EchoActor {}

#[derive(Serialize, Deserialize)]
pub struct Echo(pub String);

impl JsonMessage for Echo {
    type Result = String;
}

#[async_trait]
impl Handler<Echo> for EchoActor {
    async fn handle(&mut self, message: Echo, _ctx: &mut ActorContext) -> String {
        message.0
    }
}
