use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message};
use crate::actor::{Actor, BoxedActorRef, LocalActorRef};
use crate::remote::net::message::SessionEvent;
use crate::remote::net::proto::protocol::StreamPublish;
use crate::remote::net::StreamMessage;
use crate::remote::stream::pubsub::{
    StreamEvent, Subscription, Topic, TopicEmitter, TopicSubscriberStore,
};
use std::any::Any;
use std::collections::HashMap;
use std::marker::PhantomData;

pub struct MediatorTopic(Box<dyn TopicEmitter>);

pub struct StreamMediator {
    topics: HashMap<String, MediatorTopic>,
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

        self.topics.insert(T::topic_name().to_string(), topic);
        self
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
                let mut remote = ctx.system_mut().remote_owned();
                let nodes = remote.get_nodes().await;

                if !nodes.is_empty() {
                    let topic = T::topic_name().to_string();
                    let message = bytes.clone();

                    trace!("notifying {} nodes", nodes.len());
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

                        trace!("notified {} nodes", node_count);
                    });
                } else {
                    trace!("no nodes to notify");
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
    async fn handle(&mut self, message: PublishRaw, _ctx: &mut ActorContext) {
        if let Some(topic) = self.topics.get_mut(&message.topic) {
            topic.0.emit(&message.key, message.message).await;
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
        _ctx: &mut ActorContext,
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
                error!("incorrect topic subscriber store type, unable to add back");
                None
            }
        } else {
            None
        };

        subscription.map_or(Err(SubscribeErr::Err), |s| Ok(s))
    }
}
