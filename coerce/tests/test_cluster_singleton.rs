use async_trait::async_trait;
use coerce::actor::context::ActorContext;
use coerce::actor::message::{Handler, Message, MessageUnwrapErr, MessageWrapErr};
use coerce::actor::system::builder::ActorSystemBuilder;
use coerce::actor::system::ActorSystem;
use coerce::actor::Actor;
use coerce::remote::system::RemoteActorSystem;
use coerce::singleton::factory::SingletonFactory;
use coerce::singleton::{singleton, SingletonBuilder};
use std::time::Duration;
use tokio::time::sleep;
use tracing::Level;

mod util;

struct SingletonActor {}

impl Actor for SingletonActor {}

struct Factory {}

impl SingletonFactory for Factory {
    type Actor = SingletonActor;

    fn create(&self) -> Self::Actor {
        SingletonActor {}
    }
}

struct Echo {
    string: String,
}

#[async_trait]
impl Handler<Echo> for SingletonActor {
    async fn handle(&mut self, message: Echo, _ctx: &mut ActorContext) -> String {
        // Simple message to echo back the message property `string` as the result
        message.string
    }
}

#[tokio::test]
pub async fn test_cluster_singleton_start_and_communicate() {
    util::create_logger(Some(Level::DEBUG));

    let system = ActorSystem::builder().system_name("node-1").build();
    let system2 = ActorSystem::builder().system_name("node-2").build();

    let remote = RemoteActorSystem::builder()
        .with_tag("remote-1")
        .with_id(1)
        .with_actor_system(system)
        .configure(singleton::<Factory>)
        .configure(|h| h.with_handler::<SingletonActor, Echo>("SingletonActor.Echo"))
        .build()
        .await;

    let remote2 = RemoteActorSystem::builder()
        .with_tag("remote-2")
        .with_id(2)
        .with_actor_system(system2)
        .configure(singleton::<Factory>)
        .configure(|h| h.with_handler::<SingletonActor, Echo>("SingletonActor.Echo"))
        .build()
        .await;

    remote
        .clone()
        .cluster_worker()
        .listen_addr("localhost:30101")
        .start()
        .await;

    remote2
        .clone()
        .cluster_worker()
        .listen_addr("localhost:30102")
        .with_seed_addr("localhost:30101")
        .start()
        .await;

    let singleton = SingletonBuilder::new(remote)
        .factory(Factory {})
        .build()
        .await;

    let singleton2 = SingletonBuilder::new(remote2)
        .factory(Factory {})
        .build()
        .await;

    assert_eq!(
        singleton
            .send(Echo {
                string: "hello world".to_string()
            })
            .await,
        Ok("hello world".to_string())
    );

    assert_eq!(
        singleton2
            .send(Echo {
                string: "hello world".to_string()
            })
            .await,
        Ok("hello world".to_string())
    );
}

impl Message for Echo {
    type Result = String;

    fn as_bytes(&self) -> Result<Vec<u8>, MessageWrapErr> {
        Ok(self.string.as_bytes().to_vec())
    }

    fn from_bytes(s: Vec<u8>) -> Result<Self, MessageUnwrapErr> {
        Ok(Self {
            string: String::from_utf8(s).unwrap(),
        })
    }

    fn read_remote_result(res: Vec<u8>) -> Result<Self::Result, MessageUnwrapErr> {
        Ok(String::from_utf8(res).unwrap())
    }

    fn write_remote_result(res: Self::Result) -> Result<Vec<u8>, MessageWrapErr> {
        Ok(res.into_bytes())
    }
}
