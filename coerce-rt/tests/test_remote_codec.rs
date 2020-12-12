use coerce_rt::actor::context::ActorSystem;
use coerce_rt::remote::system::RemoteActorSystem;

use util::*;

pub mod util;

#[macro_use]
extern crate serde;
extern crate serde_json;

extern crate chrono;

#[macro_use]
extern crate async_trait;

#[tokio::test]
pub async fn test_remote_create_message() {
    util::create_trace_logger();

    let mut remote = RemoteActorSystem::builder()
        .with_actor_system(ActorSystem::new())
        .with_handlers(move |handlers| {
            handlers.with_handler::<TestActor, SetStatusRequest>("TestActor.SetStatusRequest")
        })
        .build()
        .await;

    let msg = SetStatusRequest {
        status: TestActorStatus::Active,
    };

    let actor = remote
        .inner()
        .new_anon_actor(TestActor::new())
        .await
        .unwrap();

    let message = remote
        .create_header::<TestActor, SetStatusRequest>(&actor.id, msg.clone())
        .unwrap();

    assert_eq!(message.actor_id, actor.id);
    assert_eq!(
        message.handler_type,
        "TestActor.SetStatusRequest".to_string()
    );
    assert_eq!(message.message, msg);
}
