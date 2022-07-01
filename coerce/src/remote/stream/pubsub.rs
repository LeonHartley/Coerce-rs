use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message};

use crate::actor::{Actor, LocalActorRef};
use crate::remote::net::StreamData;
use crate::remote::stream::mediator::{Publish, Reach, Subscribe, SubscribeErr};

use std::any::Any;

use std::collections::HashMap;
use std::sync::Arc;

use crate::remote::system::RemoteActorSystem;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

pub struct PubSub;

pub trait Topic: 'static + Send + Sync {
    type Message: StreamData;

    fn topic_name() -> &'static str;

    fn key(&self) -> String {
        String::default()
    }
}

pub struct Receive<T: Topic>(pub Arc<T::Message>);

impl PubSub {
    pub async fn subscribe<A: Actor, T: Topic>(
        topic: T,
        ctx: &ActorContext,
    ) -> Result<Subscription, SubscribeErr>
    where
        A: Handler<Receive<T>>,
    {
        // let topic_data = format!("{}-{}", T::topic_name(), &topic.key());
        // let span = tracing::debug_span!(
        //     "PubSub::subscribe",
        //     actor_type_name = A::type_name(),
        //     topic = topic_data.as_str()
        // );
        // let _enter = span.enter();

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

    pub async fn publish<T: Topic>(topic: T, message: T::Message, system: &RemoteActorSystem) {
        // let topic_data = format!("{}-{}", T::topic_name(), &topic.key());
        // let span = tracing::debug_span!("PubSub::publish", topic = topic_data.as_str());
        // let _enter = span.enter();
        if let Some(mediator) = system.stream_mediator() {
            let _ = mediator
                .send(Publish::<T> {
                    topic,
                    message,
                    reach: Reach::Cluster,
                })
                .await
                .unwrap();
        } else {
            panic!("no stream mediator found, system not setup for distributed streams")
        }
    }

    pub async fn publish_locally<T: Topic>(
        topic: T,
        message: T::Message,
        system: &RemoteActorSystem,
    ) {
        if let Some(mediator) = system.stream_mediator() {
            let reach = Reach::Local;

            let _ = mediator
                .send(Publish {
                    topic,
                    message,
                    reach,
                })
                .await
                .unwrap();
        } else {
            panic!("no stream mediator found, system not setup for distributed streams")
        }
    }
}

impl<T: Topic> Clone for Receive<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T: Topic> Message for Receive<T> {
    type Result = ();
}

#[async_trait]
pub trait TopicEmitter: 'static + Any + Send + Sync {
    async fn emit_serialised(&self, key: &str, bytes: Vec<u8>);

    async fn emit(&self, key: &str, msg: Arc<dyn Any + Sync + Send>);

    fn as_any(&self) -> &dyn Any;

    fn as_any_mut(&mut self) -> &mut dyn Any;
}

#[derive(Default)]
pub struct TopicSubscriberStore<T: Topic> {
    channels: HashMap<String, broadcast::Sender<Receive<T>>>,
}

impl<T: Topic> TopicSubscriberStore<T> {
    pub fn new() -> Self {
        Self {
            channels: Default::default(),
        }
    }

    pub fn receiver(&mut self, key: &str) -> broadcast::Receiver<Receive<T>> {
        match self.channels.get(key) {
            Some(channel) => channel.subscribe(),
            None => {
                let (sender, receiver) = broadcast::channel(1024);
                self.channels.insert(key.to_string(), sender);

                receiver
            }
        }
    }

    pub fn broadcast(&self, key: &str, msg: Arc<T::Message>) {
        match self.channels.get(key) {
            Some(sender) => {
                sender.send(Receive(msg));
            }
            None => {
                trace!(
                    "no subscribers, message will not be sent (topic={})",
                    T::topic_name()
                )
            }
        }
    }
}

#[async_trait]
impl<T: Topic + 'static> TopicEmitter for TopicSubscriberStore<T> {
    async fn emit_serialised(&self, key: &str, bytes: Vec<u8>) {
        // let topic_data = format!("{}-{}", T::topic_name(), &key);
        // let span = tracing::debug_span!(
        //     "PubSub::emit",
        //     topic = topic_data.as_str(),
        //     message_length = bytes.len()
        // );
        //
        // let _enter = span.enter();

        if let Some(message) = T::Message::read_from_bytes(bytes) {
            let message = Arc::new(message);
            self.broadcast(key, message);
        } else {
            // log err, possibly notify the sender?
        }
    }

    async fn emit(&self, key: &str, msg: Arc<dyn Any + Sync + Send>) {
        let msg = msg.downcast::<T::Message>().unwrap();

        self.broadcast(key, msg);
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
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
        topic_receiver: broadcast::Receiver<Receive<T>>,
        receiver_ref: LocalActorRef<A>,
    ) -> Subscription
    where
        A: Handler<Receive<T>>,
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
