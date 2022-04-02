use coerce::actor::context::ActorContext;
use opentelemetry::global;
use opentelemetry::sdk::propagation::TraceContextPropagator;

use coerce::actor::message::Handler;
use coerce::actor::system::ActorSystem;
use coerce::actor::Actor;
use coerce::remote::net::StreamData;
use coerce::remote::stream::pubsub::{PubSub, StreamEvent, Subscription, Topic};
use coerce::remote::system::RemoteActorSystem;
use tokio::sync::oneshot::{channel, Sender};
use tokio::time::Duration;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

pub mod util;

#[macro_use]
extern crate serde;

#[macro_use]
extern crate async_trait;

#[derive(Debug)]
pub enum StatusEvent {
    Online,
    Offline,
}

pub struct StatusStream;

impl Topic for StatusStream {
    type Message = StatusEvent;

    fn topic_name() -> &'static str {
        "test-topic"
    }
}

pub struct TestStreamConsumer {
    subscription: Option<Subscription>,
    expected_stream_messages: u32,
    on_completion: Option<Sender<u32>>,
    received_stream_messages: u32,
}

#[async_trait]
impl Actor for TestStreamConsumer {
    async fn started(&mut self, ctx: &mut ActorContext) {
        self.subscription = Some(
            PubSub::subscribe::<Self, StatusStream>(StatusStream, ctx)
                .await
                .unwrap(),
        );
    }
}

#[async_trait]
impl Handler<StreamEvent<StatusStream>> for TestStreamConsumer {
    async fn handle(&mut self, message: StreamEvent<StatusStream>, _ctx: &mut ActorContext) {
        match message {
            StreamEvent::Receive(msg) => {
                log::info!("received msg: {:?}", &msg);

                self.received_stream_messages += 1;

                if self.received_stream_messages == self.expected_stream_messages {
                    self.on_completion
                        .take()
                        .unwrap()
                        .send(self.received_stream_messages);
                }
            }
            StreamEvent::Err => {}
        }
    }
}

#[tokio::test]
pub async fn test_pubsub_local() {
    util::create_trace_logger();

    let sys = ActorSystem::new();
    let remote = RemoteActorSystem::builder()
        .with_actor_system(sys)
        .with_distributed_streams(|s| s.add_topic::<StatusStream>())
        .build()
        .await;

    let (sender_a, receiver_a) = channel::<u32>();
    let (sender_b, receiver_b) = channel::<u32>();

    let _actor = remote
        .actor_system()
        .new_anon_actor(TestStreamConsumer {
            subscription: None,
            expected_stream_messages: 10,
            on_completion: Some(sender_a),
            received_stream_messages: 0,
        })
        .await
        .unwrap();

    let _actor_2 = remote
        .actor_system()
        .new_anon_actor(TestStreamConsumer {
            subscription: None,
            expected_stream_messages: 10,
            on_completion: Some(sender_b),
            received_stream_messages: 0,
        })
        .await
        .unwrap();

    for _ in 0..10 {
        PubSub::publish(StatusStream, StatusEvent::Online, &remote).await;
    }

    let received_stream_messages = receiver_a.await.unwrap();
    let received_stream_messages_2 = receiver_b.await.unwrap();

    assert_eq!(received_stream_messages, 10);
    assert_eq!(received_stream_messages_2, 10);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
pub async fn test_pubsub_distributed() {
    util::create_trace_logger();

    let sys = ActorSystem::new();
    let remote = RemoteActorSystem::builder()
        .with_actor_system(sys)
        .with_id(1)
        .with_distributed_streams(|s| s.add_topic::<StatusStream>())
        .build()
        .await;

    let sys = ActorSystem::new();
    let remote_b = RemoteActorSystem::builder()
        .with_actor_system(sys)
        .with_id(2)
        .with_distributed_streams(|s| s.add_topic::<StatusStream>())
        .build()
        .await;

    remote
        .clone()
        .cluster_worker()
        .listen_addr("localhost:30101")
        .start()
        .await;

    remote_b
        .clone()
        .cluster_worker()
        .listen_addr("localhost:30102")
        .with_seed_addr("localhost:30101")
        .start()
        .await;

    let (sender_a, receiver_a) = channel::<u32>();
    let (sender_b, receiver_b) = channel::<u32>();

    let actor = remote
        .actor_system()
        .new_anon_actor(TestStreamConsumer {
            subscription: None,
            expected_stream_messages: 10,
            on_completion: Some(sender_a),
            received_stream_messages: 0,
        })
        .await
        .unwrap();

    let actor_2 = remote_b
        .actor_system()
        .new_anon_actor(TestStreamConsumer {
            subscription: None,
            expected_stream_messages: 10,
            on_completion: Some(sender_b),
            received_stream_messages: 0,
        })
        .await
        .unwrap();

    // Publish 5 messages on the first server
    for _ in 0..5 {
        PubSub::publish(StatusStream, StatusEvent::Online, &remote).await;
    }

    // Publish 5 messages on the second server
    for _ in 0..5 {
        PubSub::publish(StatusStream, StatusEvent::Online, &remote_b).await;
    }

    // ensure both actors (one on each system) receives all stream messages from both servers within 3 seconds
    if tokio::time::timeout(
        Duration::from_secs(3),
        futures::future::join_all(vec![receiver_a, receiver_b]),
    )
    .await
    .is_err()
    {
        let received_stream_messages = actor.exec(|a| a.received_stream_messages).await.unwrap();
        let received_stream_messages_2 =
            actor_2.exec(|a| a.received_stream_messages).await.unwrap();

        panic!(
            "received {}/10, {}/10",
            received_stream_messages, received_stream_messages_2
        );
    }
}

impl StreamData for StatusEvent {
    fn read_from_bytes(data: Vec<u8>) -> Option<Self> {
        match data.first() {
            Some(0) => Some(StatusEvent::Offline),
            Some(1) => Some(StatusEvent::Online),
            _ => None,
        }
    }

    fn write_to_bytes(&self) -> Option<Vec<u8>> {
        match &self {
            StatusEvent::Offline => Some(vec![0]),
            StatusEvent::Online => Some(vec![1]),
        }
    }
}
