use crate::actor::context::ActorStatus::Stopping;
use crate::actor::context::{ActorHandlerContext, ActorStatus};
use crate::actor::message::{Handler, Message};
use crate::actor::Actor;

pub struct Status {}

pub struct Stop {}

impl Message for Status {
    type Result = ActorStatus;
}

impl Message for Stop {
    type Result = ActorStatus;
}

#[async_trait]
impl<A> Handler<Status> for A
where
    A: 'static + Actor + Sync + Send,
{
    async fn handle(&mut self, message: Status, ctx: &mut ActorHandlerContext) -> ActorStatus {
        ctx.get_status().clone()
    }
}

#[async_trait]
impl<A> Handler<Stop> for A
where
    A: 'static + Actor + Sync + Send,
{
    async fn handle(&mut self, message: Stop, ctx: &mut ActorHandlerContext) -> ActorStatus {
        ctx.set_status(Stopping);

        Stopping
    }
}
