use crate::actor::context::{ActorHandlerContext, ActorStatus};
use crate::actor::message::{Handler, Message};
use crate::actor::Actor;

pub struct Stop {}

pub struct Status {}

pub enum StatusResult {
    Ok(ActorStatus),
    Err,
}

impl Message for Status {
    type Result = StatusResult;
}

impl<A> Handler<Status> for A
    where
        A: Actor,
{
    async fn handle(&mut self, message: Status, ctx: &mut ActorHandlerContext) -> StatusResult {
        StatusResult::Ok(ctx.get_status().clone())
    }
}
