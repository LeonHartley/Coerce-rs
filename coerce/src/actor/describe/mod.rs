use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message};
use crate::actor::scheduler::ActorScheduler;
use crate::actor::supervised::{ChildRef, Supervised};
use crate::actor::{
    Actor, ActorId, ActorPath, ActorRefErr, ActorTags, BoxedActorRef, CoreActorRef, IntoActorPath,
    ToActorId,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::sync::oneshot::Sender;
use tokio::time::Instant;

#[derive(Default, Debug)]
pub struct Describe {
    pub options: Arc<DescribeOptions>,
    pub sender: Option<Sender<ActorDescription>>,
    pub current_depth: usize,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DescribeOptions {
    pub max_depth: Option<usize>,
    pub max_children: Option<usize>,
    pub child_describe_timeout: Option<Duration>,
    pub child_describe_attached: bool,
}

impl Default for DescribeOptions {
    fn default() -> Self {
        Self {
            max_depth: None,
            max_children: None,
            child_describe_timeout: None,
            child_describe_attached: true,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum DescribeResult {
    Ok(ActorDescription),
    Err {
        error: ActorRefErr,
        actor_id: ActorId,
        actor_path: ActorPath,
        actor_type: String,
    },
    Timeout {
        actor_id: ActorId,
        actor_path: ActorPath,
        actor_type: String,
    },
}

impl DescribeResult {
    pub fn is_ok(&self) -> bool {
        matches!(&self, Self::Ok(_))
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ActorDescription {
    pub actor_id: ActorId,
    pub path: ActorPath,
    pub actor_type_name: String,
    pub actor_context_id: u64,
    pub tags: ActorTags,
    // pub last_message_timestamp: Option<i64>,
    pub supervised: Option<SupervisedDescription>,
    pub time_taken: Option<Duration>,
}

impl Message for Describe {
    type Result = ();
}
#[derive(Serialize, Deserialize, Debug)]
pub struct SupervisedDescription {
    pub actors: Vec<DescribeResult>,
}

#[async_trait]
impl<A: Actor> Handler<Describe> for A {
    async fn handle(&mut self, message: Describe, ctx: &mut ActorContext) {
        let start = Instant::now();
        let description = ActorDescription {
            actor_id: ctx.id().clone(),
            path: ctx.full_path().clone(),
            actor_type_name: A::type_name().to_string(),
            actor_context_id: ctx.ctx_id(),
            tags: ctx.tags(),
            // last_message_timestamp: ctx.last_message_timestamp,
            supervised: None,
            time_taken: None,
        };

        let mut message = message;
        let sender = message.sender.take().unwrap();

        if let Some(supervised) = ctx.supervised() {
            let next_depth = message.current_depth + 1;

            if let Some(max_depth) = message.options.max_depth {
                if max_depth < next_depth {
                    let _ = sender.send(description);
                    return;
                }
            }

            let describable_actors = get_describable_actors(
                supervised.children.values(),
                message.options.child_describe_attached,
                message.options.max_children,
            );

            if !describable_actors.is_empty() {
                tokio::spawn(async move {
                    let actors = describe_all(describable_actors, next_depth, &message).await;

                    let description = {
                        let mut description = description;
                        description.supervised = Some(SupervisedDescription { actors });
                        description
                    };

                    log_and_send(sender, description, start);
                });

                return;
            }
        }

        log_and_send(sender, description, start);
    }
}

#[inline]
fn log_and_send(sender: Sender<ActorDescription>, description: ActorDescription, start: Instant) {
    debug!(
        "describe(actor_id={actor_id}, {supervised_count} supervised actor(s)), took {time_taken} millis",
        actor_id = &description.actor_id,
        supervised_count = description
        .supervised
        .as_ref()
        .map_or(0, |s| s.actors.len()),
        time_taken = start.elapsed().as_millis()
    );

    let _ = sender.send(description);
}

pub async fn describe_all(
    actors: Vec<BoxedActorRef>,
    next_depth: usize,
    message: &Describe,
) -> Vec<DescribeResult> {
    let actors = actors.into_iter().map(|actor| {
        let (tx, rx) = oneshot::channel();
        let describe = {
            let mut describe = message.clone();
            describe.current_depth = next_depth;
            describe.sender = Some(tx);
            describe
        };

        let timeout = if message.current_depth > 0 {
            Duration::from_secs(5)
        } else {
            Duration::from_secs(10)
        };

        async move {
            let start = Instant::now();
            match actor.describe(describe) {
                Ok(_) => {
                    // TODO: Apply a timeout to `rx.await`
                    let description = tokio::time::timeout(timeout, rx).await;
                    if let Ok(description) = description {
                        match description {
                            Ok(description) => DescribeResult::Ok({
                                let mut description = description;
                                description.time_taken = Some(start.elapsed());
                                description
                            }),
                            Err(_) => {
                                DescribeResult::from_err(ActorRefErr::ResultChannelClosed, &actor)
                            }
                        }
                    } else {
                        DescribeResult::from_timeout_err(&actor)
                    }
                }

                Err(e) => DescribeResult::from_err(e, &actor),
            }
        }
    });

    futures::future::join_all(actors).await
}

fn get_describable_actors<'actor>(
    actors: impl Iterator<Item = &'actor ChildRef>,
    describe_attached_children: bool,
    max_children: Option<usize>,
) -> Vec<BoxedActorRef> {
    actors
        .filter(|a| describe_attached_children || !a.is_attached())
        .take(max_children.map_or_else(|| usize::MAX, |n| n))
        .map(|n| n.actor_ref().clone())
        .collect()
}

pub struct DescribeAll {
    pub options: Arc<DescribeOptions>,
    pub sender: Sender<Vec<DescribeResult>>,
}

impl Message for DescribeAll {
    type Result = ();
}

#[async_trait]
impl Handler<DescribeAll> for ActorScheduler {
    async fn handle(&mut self, message: DescribeAll, ctx: &mut ActorContext) {
        let start = Instant::now();
        let actors = {
            let mut actors: Vec<BoxedActorRef> = self.actors.values().cloned().collect();
            actors.push(ctx.boxed_actor_ref());
            actors
        };

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

impl Clone for Describe {
    fn clone(&self) -> Self {
        Self {
            options: self.options.clone(),
            sender: None,
            current_depth: self.current_depth,
        }
    }
}

impl DescribeResult {
    #[inline]
    pub fn from_err(e: ActorRefErr, actor_ref: &BoxedActorRef) -> Self {
        DescribeResult::Err {
            error: e,
            actor_id: actor_ref.actor_id().clone(),
            actor_path: actor_ref.actor_path().clone(),
            actor_type: actor_ref.actor_type().to_string(),
        }
    }

    #[inline]
    pub fn from_timeout_err(actor_ref: &BoxedActorRef) -> Self {
        DescribeResult::Timeout {
            actor_id: actor_ref.actor_id().clone(),
            actor_path: actor_ref.actor_path().clone(),
            actor_type: actor_ref.actor_type().to_string(),
        }
    }
}
