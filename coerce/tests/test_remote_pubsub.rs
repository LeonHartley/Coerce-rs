use coerce::actor::context::ActorContext;
use coerce::actor::lifecycle::Status;
use coerce::actor::message::encoding::json::RemoteMessage;
use coerce::actor::message::{Handler, Message};
use coerce::actor::system::ActorSystem;
use coerce::actor::{new_actor, Actor};
use coerce::remote::net::StreamMessage;
use coerce::remote::stream::mediator::{PublishErr, PublishRaw, StreamMediator, Subscribe};
use coerce::remote::stream::pubsub::{PubSub, StreamEvent, Topic};
use tokio::time::Duration;

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

pub struct TestStreamConsumer {}

#[async_trait]
impl Actor for TestStreamConsumer {
    async fn started(&mut self, ctx: &mut ActorContext) {
        PubSub::subscribe::<Self, StatusStream>(ctx).await.unwrap()
    }

    async fn stopped(&mut self, ctx: &mut ActorContext) {
        PubSub::unsubscribe::<Self, StatusStream>(ctx)
            .await
            .unwrap()
    }
}

#[async_trait]
impl Handler<StreamEvent<StatusStream>> for TestStreamConsumer {
    async fn handle(&mut self, message: StreamEvent<StatusStream>, ctx: &mut ActorContext) {
        match message {
            StreamEvent::Receive(msg) => log::info!("received msg: {:?}", &msg),
            StreamEvent::Err => {}
        }
    }
}

#[tokio::test]
pub async fn test_pubsub_mediator() {
    util::create_trace_logger();

    let mut sys = ActorSystem::new();

    let actor = sys.new_tracked_actor(TestStreamConsumer {}).await.unwrap();
    let mut mediator = sys
        .new_anon_actor(StreamMediator::new().add_topic::<StatusStream>())
        .await
        .unwrap();

    mediator
        .send(Subscribe::new(StatusStream, actor))
        .await
        .unwrap();

    for i in 0..10 {
        mediator
            .send(PublishRaw {
                topic: "test-topic".to_string(),
                key: String::default(),
                message: vec![1],
            })
            .await
            .unwrap();
    }
}

impl StreamMessage for StatusEvent {
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
