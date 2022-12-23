use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message};
use crate::actor::{Actor, LocalActorRef};
use crate::remote::actor::message::SetRemote;
use crate::remote::heartbeat::Heartbeat;
use crate::remote::net::message::SessionEvent;
use crate::remote::net::proto::network::StreamPublishEvent;
use crate::remote::net::StreamData;
use crate::remote::stream::pubsub::{
    Receive, Subscription, Topic, TopicEmitter, TopicSubscriberStore,
};
use crate::remote::stream::system::{ClusterEvent, SystemEvent, SystemTopic};
use crate::remote::system::{NodeId, RemoteActorSystem};
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

pub struct MediatorTopic(Box<dyn TopicEmitter>);

#[derive(Default)]
pub struct StreamMediator {
    remote: Option<RemoteActorSystem>,
    nodes: HashSet<NodeId>,
    topics: HashMap<String, MediatorTopic>,
    system_subscription: Option<Subscription>,
}

impl StreamMediator {
    pub fn new() -> StreamMediator {
        Self::default()
    }

    fn remote(&self) -> &RemoteActorSystem {
        self.remote
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

pub enum Reach {
    Local,
    Cluster,
}

pub struct Publish<T: Topic> {
    pub topic: T,
    pub message: T::Message,
    pub reach: Reach,
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
    pub fn subscribe<A: Actor, T: Topic>(
        &mut self,
        topic: T,
        receiver_ref: LocalActorRef<A>,
    ) -> Result<Subscription, SubscribeErr>
    where
        A: Handler<Receive<T>>,
    {
        let topic_key = &topic.key();
        let receiver = match self.topics.entry(T::topic_name().to_string()) {
            Entry::Occupied(occupied) => occupied.into_mut(),
            Entry::Vacant(vacant_entry) => {
                let topic = MediatorTopic::new::<T>();
                vacant_entry.insert(topic)
            }
        };

        if let Some(topic) = receiver.subscriber_store_mut() {
            debug!(
                "actor_id={} subscribing to topic {} (key=\"{}\")",
                receiver_ref.actor_id(),
                T::topic_name().to_string(),
                &topic_key
            );

            let receiver = topic.receiver(topic_key);
            let subscription = Subscription::new(receiver, receiver_ref);

            Ok(subscription)
        } else {
            error!(
                "actor_id={} failed to subscribe to topic {} (key=\"{}\")",
                &receiver_ref.actor_id(),
                T::topic_name().to_string(),
                &topic_key
            );

            Err(SubscribeErr::Err)
        }
    }
}

#[async_trait]
impl Handler<SetRemote> for StreamMediator {
    async fn handle(&mut self, message: SetRemote, ctx: &mut ActorContext) {
        Heartbeat::register(ctx.boxed_actor_ref(), &message.0);

        self.remote = Some(message.0);
        self.system_subscription = Some(self.subscribe(SystemTopic, self.actor_ref(ctx)).unwrap())
    }
}

#[async_trait]
impl Handler<Receive<SystemTopic>> for StreamMediator {
    async fn handle(&mut self, message: Receive<SystemTopic>, _ctx: &mut ActorContext) {
        match message.0.as_ref() {
            SystemEvent::Cluster(cluster_event) => match cluster_event {
                ClusterEvent::NodeAdded(new_node) => {
                    if new_node.id != self.remote().node_id() {
                        self.nodes.insert(new_node.id);
                    }

                    info!("node added (node_id={})", new_node.id);
                }

                ClusterEvent::NodeRemoved(removed_node) => {
                    // TODO: instead of just removing a node when
                    //       we receive notification that a node was removed from the cluster,
                    //       we could potentially start buffering any messages that have been published
                    //       with a configurable TTL so that if/when the node rejoins the cluster,
                    //       it will receive any messages it may have missed.

                    let _ = self.nodes.remove(&removed_node.id);

                    info!("node removed (node_id={})", removed_node.id);
                }
                _ => {}
            },
        }
    }
}

#[async_trait]
impl<T: Topic> Handler<Publish<T>> for StreamMediator {
    async fn handle(
        &mut self,
        message: Publish<T>,
        _ctx: &mut ActorContext,
    ) -> Result<(), PublishErr> {
        let msg = Arc::new(message.message);
        if let Some(topic) = self.topics.get(T::topic_name()) {
            topic.0.emit(&message.topic.key(), msg.clone()).await;
        }

        if message.reach.remote_publish() && !self.nodes.is_empty() {
            match msg.write_to_bytes() {
                Some(bytes) => {
                    let remote = self.remote().clone();
                    let topic = T::topic_name().to_string();
                    let key = message.topic.key();
                    let nodes: Vec<NodeId> = self.nodes.iter().copied().collect();

                    tokio::spawn(async move {
                        let message = bytes;
                        let publish = Arc::new(StreamPublishEvent {
                            topic,
                            message,
                            key,
                            ..Default::default()
                        });

                        let node_count = nodes.len();
                        for node in nodes {
                            let _ = remote
                                .notify_node(node, SessionEvent::StreamPublish(publish.clone()))
                                .await;
                        }

                        debug!("notified {} nodes", node_count);
                    });
                    Ok(())
                }
                None => Err(PublishErr::SerializationErr),
            }
        } else {
            Ok(())
        }
    }
}

impl From<Arc<StreamPublishEvent>> for PublishRaw {
    fn from(s: Arc<StreamPublishEvent>) -> Self {
        match Arc::<StreamPublishEvent>::try_unwrap(s) {
            Ok(p) => {
                let topic = p.topic;
                let key = p.key;
                let message = p.message;

                Self {
                    topic,
                    key,
                    message,
                }
            }
            Err(s) => {
                let topic = s.topic.clone();
                let key = s.key.clone();
                let message = s.message.clone();

                Self {
                    topic,
                    key,
                    message,
                }
            }
        }
    }
}

#[async_trait]
impl Handler<PublishRaw> for StreamMediator {
    async fn handle(&mut self, message: PublishRaw, _ctx: &mut ActorContext) {
        if let Some(topic) = self.topics.get(&message.topic) {
            topic.0.emit_serialised(&message.key, message.message).await;
        } else {
            trace!("no topic: {}", &message.topic)
        }
    }
}

#[async_trait]
impl<A: Actor, T: Topic> Handler<Subscribe<A, T>> for StreamMediator
where
    A: Handler<Receive<T>>,
{
    async fn handle(
        &mut self,
        message: Subscribe<A, T>,
        _ctx: &mut ActorContext,
    ) -> Result<Subscription, SubscribeErr> {
        self.subscribe(message.topic, message.receiver_ref)
    }
}

impl Reach {
    pub fn remote_publish(&self) -> bool {
        match &self {
            Self::Local => false,
            Self::Cluster => true,
        }
    }
}

impl MediatorTopic {
    pub fn new<T: Topic>() -> Self {
        let subscriber_store = TopicSubscriberStore::<T>::new();
        MediatorTopic(Box::new(subscriber_store))
    }

    pub fn subscriber_store_mut<T: Topic>(&mut self) -> Option<&mut TopicSubscriberStore<T>> {
        self.0.as_any_mut().downcast_mut()
    }

    pub fn subscriber_store<T: Topic>(&mut self) -> Option<&TopicSubscriberStore<T>> {
        self.0.as_any().downcast_ref()
    }
}
