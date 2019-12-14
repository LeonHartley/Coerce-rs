use crate::actor::context::{ActorHandlerContext};






use uuid::Uuid;

pub mod context;
pub mod lifecycle;
pub mod message;
pub mod scheduler;

pub type ActorId = Uuid;

#[async_trait]
pub trait Actor {
    async fn started(&mut self, _ctx: &mut ActorHandlerContext) {
        println!("actor started");
    }

    async fn stopped(&mut self, _ctx: &mut ActorHandlerContext) {
        println!("actor stopped");
    }
}
