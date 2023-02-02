use coerce::actor::context::ActorContext;
use coerce::actor::message::Handler;
use coerce::actor::Actor;
use coerce_macros::JsonMessage;

pub struct EchoActor;

impl Actor for EchoActor {}

#[derive(JsonMessage, Serialize, Deserialize)]
#[result("String")]
pub struct Echo(pub String);

#[async_trait]
impl Handler<Echo> for EchoActor {
    async fn handle(&mut self, message: Echo, _ctx: &mut ActorContext) -> String {
        message.0
    }
}
