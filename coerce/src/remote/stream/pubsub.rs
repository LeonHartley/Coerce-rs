use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message};
use crate::actor::system::ActorSystem;
use crate::actor::{Actor, ActorId, LocalActorRef};
use crate::remote::net::StreamMessage;
use crate::remote::stream::mediator::SubscribeErr;
use serde::export::PhantomData;
use std::collections::HashMap;
use std::sync::Arc;

pub struct PubSub;

pub trait Topic: 'static + Send + Sync {
    type Message: StreamMessage;

    fn topic_name() -> &'static str;
}

pub enum StreamEvent<T: Topic> {
    Receive(Arc<T::Message>),
    Err,
}

impl<T: Topic> Message for StreamEvent<T> {
    type Result = ();
}

#[async_trait]
pub trait TopicEmitter: Send + Sync {
    async fn emit(&mut self, bytes: Vec<u8>);

    fn add_subscriber(&mut self);
}

pub trait Subscriber<T: Topic>: Send + Sync {
    fn send(&mut self, message: StreamEvent<T>);
}

pub struct TopicSubscriber<A: Actor, T: Topic>
where
    A: Handler<StreamEvent<T>>,
{
    actor_ref: LocalActorRef<A>,
    _t: PhantomData<T>,
}

impl<A: Actor, T: Topic> Subscriber<T> for TopicSubscriber<A, T>
where
    A: Handler<StreamEvent<T>>,
{
    fn send(&mut self, message: StreamEvent<T>) {
        self.actor_ref.notify(message).unwrap();
    }
}

pub struct TopicSubscriberStore<T: Topic> {
    subscribers: HashMap<ActorId, Box<dyn Subscriber<T>>>,
}

impl<T: Topic> TopicSubscriberStore<T> {
    pub fn broadcast(&mut self, msg: Arc<T::Message>) {
        for subscriber in self.subscribers.values_mut() {
            subscriber.send(StreamEvent::Receive(msg.clone()));
        }
    }

    pub fn broadcast_err(&mut self) {
        for subscriber in self.subscribers.values_mut() {
            subscriber.send(StreamEvent::Err);
        }
    }
}

#[async_trait]
impl<T: Topic> TopicEmitter for TopicSubscriberStore<T> {
    async fn emit(&mut self, bytes: Vec<u8>) {
        if let Some(message) = T::Message::read_from_bytes(bytes) {
            let message = Arc::new(message);
            self.broadcast(message);
        } else {
            self.broadcast_err()
        }
    }
}

impl PubSub {
    pub async fn subscribe<A: Actor, T: Topic>(ctx: &mut ActorContext) -> Result<(), SubscribeErr>
    where
        A: Handler<StreamEvent<T>>,
    {
        Ok(())
    }

    pub async fn unsubscribe<A: Actor, T: Topic>(
        ctx: &mut ActorContext,
    ) -> Result<(), SubscribeErr> {
        unimplemented!()
    }

    pub async fn publish<T: Topic>(message: T::Message, system: &mut ActorSystem) {
        unimplemented!()
    }
}
