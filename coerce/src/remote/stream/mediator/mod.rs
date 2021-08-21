use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message};
use crate::actor::{Actor, LocalActorRef};
use crate::remote::actor::message::SetRemote;
use crate::remote::net::message::SessionEvent;
use crate::remote::net::proto::protocol::StreamPublish;
use crate::remote::net::StreamMessage;
use crate::remote::stream::pubsub::{
    StreamEvent, Subscription, Topic, TopicEmitter, TopicSubscriberStore,
};
use crate::remote::system::RemoteActorSystem;

use std::collections::HashMap;

pub struct MediatorTopic(Box<dyn TopicEmitter>);

pub struct StreamMediator {
    remote: Option<RemoteActorSystem>,
    topics: HashMap<String, MediatorTopic>,
}

impl StreamMediator {
    pub fn new() -> StreamMediator {
        StreamMediator {
            remote: None,
            topics: HashMap::new(),
        }
    }

    fn remote(&self) -> &RemoteActorSystem {
        &self
            .remote
            .as_ref()
            .expect("StreamMediator remote actor system not set")
    }
}

impl Actor for StreamMediator {}

#[derive(Debug)]
pub enum SubscribeErr {
    Err,
}

pub struct Subscribe<A: Actor, T: Topic> {
    receiver_ref: LocalActorRef<A>,
    topic: T,
}

impl<A: Actor, T: Topic> Subscribe<A, T> {
    pub fn new(topic: T, receiver_ref: LocalActorRef<A>) -> Self {
        Subscribe {
            receiver_ref,
            topic,
        }
    }
}

#[derive(Debug)]
pub enum PublishErr {
    SerializationErr,
}

pub struct Publish<T: Topic> {
    pub topic: T,
    pub message: T::Message,
}

pub struct PublishRaw {
    pub topic: String,
    pub key: String,
    pub message: Vec<u8>,
}

impl<A: Actor, T: Topic> Message for Subscribe<A, T> {
    type Result = Result<Subscription, SubscribeErr>;
}

impl<T: Topic> Message for Publish<T> {
    type Result = Result<(), PublishErr>;
}

impl Message for PublishRaw {
    type Result = ();
}

impl StreamMediator {
    pub fn add_topic<T: Topic>(&mut self) -> &mut Self {
        let subscriber_store = TopicSubscriberStore::<T>::new();
        let topic = MediatorTopic(Box::new(subscriber_store));

        let topic_name = T::topic_name().to_string();
        self.topics.insert(topic_name, topic);
        self
    }
}

#[async_trait]
impl Handler<SetRemote> for StreamMediator {
    async fn handle(&mut self, message: SetRemote, _ctx: &mut ActorContext) {
        self.remote = Some(message.0);
    }
}

#[async_trait]
impl<T: Topic> Handler<Publish<T>> for StreamMediator {
    async fn handle(
        &mut self,
        message: Publish<T>,
        ctx: &mut ActorContext,
    ) -> Result<(), PublishErr> {
        match message.message.write_to_bytes() {
            Some(bytes) => {
                let remote = self.remote().clone();
                let nodes = remote.get_nodes().await;

                if !nodes.is_empty() {
                    let topic = T::topic_name().to_string();
                    let message = bytes.clone();

                    trace!(ctx.log(), "notifying {} nodes", nodes.len());
                    let log = ctx.log().clone();
                    tokio::spawn(async move {
                        let publish = StreamPublish {
                            topic,
                            message,
                            ..StreamPublish::default()
                        };

                        let node_count = nodes.len();
                        for node in nodes {
                            if node.id != remote.node_id() {
                                remote
                                    .send_message(
                                        node.id,
                                        SessionEvent::StreamPublish(publish.clone()),
                                    )
                                    .await;
                            }
                        }

                        trace!(&log, "notified {} nodes", node_count);
                    });
                } else {
                    trace!(ctx.log(), "no nodes to notify");
                }

                if let Some(topic) = self.topics.get_mut(T::topic_name()) {
                    topic.0.emit(&message.topic.key(), bytes).await;
                    Ok(())
                } else {
                    Ok(())
                }
            }
            None => Err(PublishErr::SerializationErr),
        }
    }
}

impl From<StreamPublish> for PublishRaw {
    fn from(s: StreamPublish) -> Self {
        let topic = s.topic;
        let key = s.key;
        let message = s.message;

        Self {
            topic,
            key,
            message,
        }
    }
}

#[async_trait]
impl Handler<PublishRaw> for StreamMediator {
    async fn handle(&mut self, message: PublishRaw, ctx: &mut ActorContext) {
        if let Some(topic) = self.topics.get_mut(&message.topic) {
            topic.0.emit(&message.key, message.message).await;
        } else {
            trace!(ctx.log(), "no topic: {}", &message.topic)
        }
    }
}

#[async_trait]
impl<A: Actor, T: Topic> Handler<Subscribe<A, T>> for StreamMediator
where
    A: Handler<StreamEvent<T>>,
{
    async fn handle(
        &mut self,
        message: Subscribe<A, T>,
        ctx: &mut ActorContext,
    ) -> Result<Subscription, SubscribeErr> {
        let subscription = if let Some(topic) = self.topics.remove(T::topic_name()) {
            let topic = topic.0.into_any().downcast::<TopicSubscriberStore<T>>();

            if let Ok(mut topic) = topic {
                let receiver = topic.receiver(&message.topic.key());
                let subscription = Subscription::new(receiver, message.receiver_ref);

                self.topics
                    .insert(T::topic_name().to_string(), MediatorTopic(topic));

                Some(subscription)
            } else {
                error!(
                    ctx.log(),
                    "incorrect topic subscriber store type, unable to add back"
                );
                None
            }
        } else {
            None
        };

        subscription.map_or(Err(SubscribeErr::Err), |s| Ok(s))
    }
}
