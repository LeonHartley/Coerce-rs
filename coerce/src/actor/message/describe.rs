use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message};
use crate::actor::supervised::{ChildRef, Supervised};
use crate::actor::{Actor, ActorId, ActorRefErr, ActorTags, BoxedActorRef, CoreActorRef};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::sync::oneshot::Sender;
use tokio::time::Instant;

#[derive(Default)]
pub struct Describe {
    pub options: Arc<DescribeOptions>,
    pub sender: Option<Sender<ActorDescription>>,
    pub current_depth: usize,
}

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

#[derive(Debug)]
pub enum DescribeResult {
    Ok(ActorDescription),
    Err {
        error: ActorRefErr,
        actor_id: ActorId,
        actor_type: &'static str,
    },
    Timeout {
        actor_id: ActorId,
        actor_type: &'static str,
    },
}

#[derive(Debug)]
pub struct ActorDescription {
    actor_id: ActorId,
    actor_type_name: &'static str,
    tags: ActorTags,
    supervised: Option<SupervisedDescription>,
}

impl Message for Describe {
    type Result = ();
}

#[derive(Debug)]
pub struct SupervisedDescription {
    actors: Vec<DescribeResult>,
}

#[async_trait]
impl<A: Actor> Handler<Describe> for A {
    async fn handle(&mut self, message: Describe, ctx: &mut ActorContext) {
        let start = Instant::now();

        let description = ActorDescription {
            actor_id: ctx.id().clone(),
            actor_type_name: A::type_name(),
            tags: ctx.tags(),
            supervised: None,
        };

        let mut message = message;
        let sender = message.sender.take().unwrap();

        if let Some(supervised) = ctx.supervised() {
            let describable_actors = get_describable_actors(
                supervised.children.values(),
                message.options.child_describe_attached,
                message.options.max_children,
            );

            let next_depth = message.current_depth + 1;

            if message.options.max_depth > Some(next_depth) {
                let _ = sender.send(description);
                return;
            }

            if !describable_actors.is_empty() {
                tokio::spawn(async move {
                    let actors = describe_all(describable_actors, next_depth, &message).await;

                    let mut description = description;
                    description.supervised = Some(SupervisedDescription { actors });

                    let _ = sender.send(description);
                });
            } else {
                let _ = sender.send(description);
            }
        } else {
            let _ = sender.send(description);
        }
    }
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

        async move {
            match actor.describe(describe) {
                Ok(_) => {
                    // TODO: Apply a timeout to `rx.await`
                    let description = rx.await;
                    match description {
                        Ok(description) => DescribeResult::Ok(description),
                        Err(_) => {
                            DescribeResult::from_err(ActorRefErr::ResultChannelClosed, &actor)
                        }
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
            actor_type: actor_ref.actor_type(),
        }
    }

    #[inline]
    pub fn from_timeout_err(actor_ref: &BoxedActorRef) -> Self {
        DescribeResult::Timeout {
            actor_id: actor_ref.actor_id().clone(),
            actor_type: actor_ref.actor_type(),
        }
    }
}
