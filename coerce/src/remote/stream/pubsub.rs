use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message};
use crate::actor::system::ActorSystem;
use crate::actor::{Actor, ActorId, BoxedActorRef, LocalActorRef};
use crate::remote::net::StreamMessage;
use crate::remote::stream::mediator::SubscribeErr;
use serde::export::PhantomData;
use std::any::Any;
use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::sync::Arc;

pub struct PubSub;

pub trait Topic: 'static + Send + Sync {
    type Message: StreamMessage;

    fn topic_name() -> &'static str;

    fn key(&self) -> String {
        String::default()
    }
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
    async fn emit(&mut self, key: &str, bytes: Vec<u8>);

    fn into_any(self: Box<Self>) -> Box<dyn Any>;
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

impl<A: Actor, T: Topic> TopicSubscriber<A, T>
where
    A: Handler<StreamEvent<T>>,
{
    pub fn new(actor_ref: LocalActorRef<A>) -> Self {
        let _t = PhantomData;
        TopicSubscriber { actor_ref, _t }
    }
}

pub struct TopicSubscriberStore<T: Topic> {
    subscribers: HashMap<String, HashMap<ActorId, Box<dyn Subscriber<T>>>>,
}

impl<T: Topic> TopicSubscriberStore<T> {
    pub fn new() -> Self {
        TopicSubscriberStore {
            subscribers: HashMap::new(),
        }
    }

    pub fn add_subscriber<A: Actor>(&mut self, key: String, actor_ref: LocalActorRef<A>)
    where
        A: Handler<StreamEvent<T>>,
    {
        let id = actor_ref.id.clone();
        let subscriber = TopicSubscriber::new(actor_ref);

        match self.subscribers.get_mut(&key) {
            Some(key_subscribers) => {
                key_subscribers.insert(id, Box::new(subscriber) as Box<dyn Subscriber<T>>);
            }
            None => {
                let mut subs = HashMap::new();

                subs.insert(id, Box::new(subscriber) as Box<dyn Subscriber<T>>);
                self.subscribers.insert(key.clone(), subs);
            }
        };
    }

    pub fn broadcast(&mut self, key: &str, msg: Arc<T::Message>) {
        match self.subscribers.get_mut(key) {
            Some(subscribers) => {
                for subscriber in subscribers.values_mut() {
                    subscriber.send(StreamEvent::Receive(msg.clone()));
                }
            }
            None => trace!(
                target: &format!("PubSub-topic-{}-{}", T::topic_name(), key),
                "no subscribers, message will not be sent"
            ),
        }
    }

    pub fn broadcast_err(&mut self, key: &str) {
        match self.subscribers.get_mut(key) {
            Some(subscribers) => {
                for subscriber in subscribers.values_mut() {
                    subscriber.send(StreamEvent::Err);
                }
            }
            None => {}
        }
    }
}

#[async_trait]
impl<T: Topic> TopicEmitter for TopicSubscriberStore<T> {
    async fn emit(&mut self, key: &str, bytes: Vec<u8>) {
        if let Some(message) = T::Message::read_from_bytes(bytes) {
            let message = Arc::new(message);
            self.broadcast(key, message);
        } else {
            self.broadcast_err(key)
        }
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
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
