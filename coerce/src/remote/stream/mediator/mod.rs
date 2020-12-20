use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message};
use crate::actor::{Actor, LocalActorRef};
use crate::remote::net::StreamMessage;
use crate::remote::stream::pubsub::{Topic, TopicEmitter};
use std::any::Any;
use std::collections::HashMap;
use std::marker::PhantomData;
use crate::remote::net::message::SessionEvent;
use crate::remote::net::proto::protocol::StreamPublish;

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
pub enum PublishErr {
    SerializationErr,
}

pub struct Publish<T: Topic> {
    message: T::Message,
}
pub struct PublishRaw {
    topic: String,
    message: Vec<u8>,
}

impl<A: Actor, T: Topic> Message for Subscribe<A, T> {
    type Result = Result<(), SubscribeErr>;
}

impl<T: Topic> Message for Publish<T> {
    type Result = Result<(), PublishErr>;
}

impl Message for PublishRaw {
    type Result = ();
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
                    let bytes_clone = bytes.clone();
                    tokio::spawn(async move {
                        for node in nodes {
                            remote.send_message(node.id, SessionEvent::StreamPublish(StreamPublish {
                                topic: T::topic_name().to_string(),
                                message: bytes_clone.clone(),
                                ..StreamPublish::default()
                            })).await;
                        }
                    });
                }

                if let Some(topic) = self.topics.get_mut(T::topic_name()) {
                    topic.0.emit(bytes);
                    Ok(())
                } else {
                    Ok(())
                }
            }
            None => Err(PublishErr::SerializationErr),
        }
    }
}

#[async_trait]
impl Handler<PublishRaw> for StreamMediator {
    async fn handle(&mut self, message: PublishRaw, _ctx: &mut ActorContext) {
        if let Some(topic) = self.topics.get_mut(&message.topic) {
            topic.0.emit(message.message);
        }
    }
}

#[async_trait]
impl<A: Actor, T: Topic> Handler<Subscribe<A, T>> for StreamMediator {
    async fn handle(
        &mut self,
        message: Subscribe<A, T>,
        _ctx: &mut ActorContext,
    ) -> Result<(), SubscribeErr> {
        if let Some(topic) = self.topics.get_mut(T::topic_name()) {
            topic.
        }

        Ok(())
    }
}
