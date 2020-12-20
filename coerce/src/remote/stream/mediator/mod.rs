use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message};
use crate::actor::{Actor, LocalActorRef};
use crate::remote::stream::pubsub::{Topic, TopicEmitter};
use std::collections::HashMap;
use std::marker::PhantomData;

pub struct MediatorTopic(Box<dyn TopicEmitter>);

pub struct StreamMediator {
    topics: HashMap<&'static str, MediatorTopic>,
}

impl StreamMediator {
    pub fn new() -> StreamMediator {
        StreamMediator {
            topics: HashMap::new(),
        }
    }
}

impl Actor for StreamMediator {}

#[derive(Debug)]
pub enum SubscribeErr {}

pub struct Subscribe<A: Actor, T: Topic> {
    receiver_ref: LocalActorRef<A>,
    _t: PhantomData<T>,
}

impl<A: Actor, T: Topic> Subscribe<A, T> {
    pub fn new(receiver_ref: LocalActorRef<A>) -> Self {
        Subscribe {
            receiver_ref,
            _t: PhantomData,
        }
    }
}

#[derive(Debug)]
pub enum PublishErr {}

pub struct Publish<T: Topic> {
    message: T::Message,
}

impl<A: Actor, T: Topic> Message for Subscribe<A, T> {
    type Result = Result<(), SubscribeErr>;
}

impl<T: Topic> Message for Publish<T> {
    type Result = Result<(), PublishErr>;
}

#[async_trait]
impl<T: Topic> Handler<Publish<T>> for StreamMediator {
    async fn handle(
        &mut self,
        message: Publish<T>,
        ctx: &mut ActorContext,
    ) -> Result<(), PublishErr> {
        unimplemented!()
    }
}

#[async_trait]
impl<A: Actor, T: Topic> Handler<Subscribe<A, T>> for StreamMediator {
    async fn handle(
        &mut self,
        message: Subscribe<A, T>,
        ctx: &mut ActorContext,
    ) -> Result<(), SubscribeErr> {
        unimplemented!()
    }
}
