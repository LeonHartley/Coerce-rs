use crate::actor::context::ActorContext;
use crate::actor::message::describe::{describe_all, Describe, DescribeOptions, DescribeResult};
use crate::actor::message::{Handler, Message};
use crate::actor::scheduler::ActorScheduler;
use crate::actor::{BoxedActorRef, CoreActorRef};
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio::time::Instant;

pub struct DescribeAll {
    pub options: Arc<DescribeOptions>,
    pub sender: oneshot::Sender<Vec<DescribeResult>>,
}

impl Message for DescribeAll {
    type Result = ();
}

#[async_trait]
impl Handler<DescribeAll> for ActorScheduler {
    async fn handle(&mut self, message: DescribeAll, _ctx: &mut ActorContext) {
        let start = Instant::now();
        let actors: Vec<BoxedActorRef> = self.actors.values().cloned().collect();

        trace!("describing actors (count={})", actors.len());
        tokio::spawn(async move {
            let describe = Describe {
                options: message.options,
                sender: None,
                current_depth: 0,
            };

            let actors = describe_all(actors, 1, &describe).await;
            let duration = start.elapsed();

            trace!(
                "actor description (count={}) took {} millis",
                actors.len(),
                duration.as_millis()
            );
            let _ = message.sender.send(actors);
        });
    }
}
