use coerce_remote::context::RemoteActorContext;
use coerce_rt::actor::context::ActorContext;
use coerce_rt::actor::scheduler::ActorType::Anonymous;
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

    let mut remote = RemoteActorContext::builder()
        .with_actor_context(ActorContext::new())
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
        .new_actor(TestActor::new(), Anonymous)
        .await
        .unwrap();

    let message = remote.create_message::<TestActor, SetStatusRequest>(actor.id, msg.clone()).await.unwrap();

    assert_eq!(message.actor_id, actor.id);
    assert_eq!(
        message.handler_type,
        "TestActor.SetStatusRequest".to_string()
    );
    assert_eq!(message.message, msg);
}
