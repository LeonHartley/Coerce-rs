use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message};
use crate::actor::system::ActorSystem;
use crate::actor::{Actor, LocalActorRef};
use crate::remote::net::StreamMessage;
use crate::remote::stream::mediator::{Publish, PublishRaw, Subscribe, SubscribeErr};

use std::any::Any;

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::broadcast;
use tokio::task::JoinHandle;

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

impl<T: Topic> Clone for StreamEvent<T> {
    fn clone(&self) -> Self {
        match &self {
            StreamEvent::Receive(msg) => StreamEvent::Receive(msg.clone()),
            StreamEvent::Err => StreamEvent::Err,
        }
    }
}

impl<T: Topic> Message for StreamEvent<T> {
    type Result = ();
}

#[async_trait]
pub trait TopicEmitter: Send + Sync {
    async fn emit(&mut self, key: &str, bytes: Vec<u8>);

    fn into_any(self: Box<Self>) -> Box<dyn Any>;
}

pub struct TopicSubscriberStore<T: Topic> {
    channels: HashMap<String, broadcast::Sender<StreamEvent<T>>>,
}

impl<T: Topic> TopicSubscriberStore<T> {
    pub fn new() -> Self {
        TopicSubscriberStore {
            channels: HashMap::new(),
        }
    }

    pub fn receiver(&mut self, key: &str) -> broadcast::Receiver<StreamEvent<T>> {
        match self.channels.get(key) {
            Some(channel) => channel.subscribe(),
            None => {
                let (sender, receiver) = broadcast::channel(1024);
                self.channels.insert(key.to_string(), sender);

                receiver
            }
        }
    }

    pub fn broadcast(&mut self, key: &str, msg: Arc<T::Message>) {
        match self.channels.get_mut(key) {
            Some(sender) => {
                sender.send(StreamEvent::Receive(msg));
            }
            None => {}
        }
    }

    pub fn broadcast_err(&mut self, key: &str) {
        match self.channels.get_mut(key) {
            Some(sender) => {
                sender.send(StreamEvent::Err);
            }
            None => (),
        }
    }
}

#[async_trait]
impl<T: Topic> TopicEmitter for TopicSubscriberStore<T> {
    async fn emit(&mut self, key: &str, bytes: Vec<u8>) {
        let topic_data = format!("{}-{}", T::topic_name(), &key);
        let span = tracing::debug_span!(
            "PubSub::emit",
            topic = topic_data.as_str(),
            message_length = bytes.len()
        );
        let _enter = span.enter();

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

pub struct Subscription {
    task_handle: Option<JoinHandle<()>>,
}

impl Subscription {
    pub fn unsubscribe(&mut self) {
        if let Some(handle) = self.task_handle.take() {
            handle.abort();
        }
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.unsubscribe();
    }
}

impl Subscription {
    pub(crate) fn new<A: Actor, T: Topic>(
        topic_receiver: broadcast::Receiver<StreamEvent<T>>,
        receiver_ref: LocalActorRef<A>,
    ) -> Subscription
    where
        A: Handler<StreamEvent<T>>,
    {
        let task_handle = Some(tokio::spawn(async move {
            let receiver_ref = receiver_ref;
            let mut stream_receiver = topic_receiver;
            while let Ok(message) = stream_receiver.recv().await {
                receiver_ref
                    .notify(message)
                    .expect("unable to notify receiver ref");
            }
        }));

        Subscription { task_handle }
    }
}

impl PubSub {
    pub async fn subscribe<A: Actor, T: Topic>(
        topic: T,
        ctx: &ActorContext,
    ) -> Result<Subscription, SubscribeErr>
    where
        A: Handler<StreamEvent<T>>,
    {
        let topic_data = format!("{}-{}", T::topic_name(), &topic.key());
        let span = tracing::debug_span!(
            "PubSub::subscribe",
            actor_type_name = A::type_name(),
            topic = topic_data.as_str()
        );
        let _enter = span.enter();

        let system = ctx.system().remote();
        if let Some(mediator) = system.stream_mediator() {
            mediator
                .send(Subscribe::<A, T>::new(topic, ctx.actor_ref()))
                .await
                .unwrap()
        } else {
            panic!("no stream mediator found, system not setup for distributed streams")
        }
    }

    pub async fn publish<T: Topic>(topic: T, message: T::Message, system: &ActorSystem) {
        let topic_data = format!("{}-{}", T::topic_name(), &topic.key());
        let span = tracing::debug_span!("PubSub::publish", topic = topic_data.as_str());
        let _enter = span.enter();

        if let Some(mediator) = system.remote().stream_mediator() {
            mediator
                .send(Publish::<T> { topic, message })
                .await
                .unwrap();
        } else {
            panic!("no stream mediator found, system not setup for distributed streams")
        }
    }

    pub async fn publish_locally<T: Topic>(topic: T, message: T::Message, system: &ActorSystem) {
        if let Some(mediator) = system.remote().stream_mediator() {
            let key = topic.key();
            let topic = T::topic_name().to_string();
            let message = message.write_to_bytes().unwrap();

            mediator
                .send(PublishRaw {
                    topic,
                    key,
                    message,
                })
                .await
                .unwrap();
        } else {
            panic!("no stream mediator found, system not setup for distributed streams")
        }
    }
}
