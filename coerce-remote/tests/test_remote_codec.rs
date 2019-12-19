use coerce_remote::context::RemoteActorContext;
use coerce_rt::actor::context::ActorContext;
use std::mem::forget;
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

    let test_set_status = "TestActor.SetStatusRequest".to_string();

    let mut remote = RemoteActorContext::builder()
        .with_actor_context(ActorContext::new())
        .with_handler::<TestActor, SetStatusRequest>(test_set_status.clone().as_ref())
        .build()
        .await;

    let msg = SetStatusRequest {
        status: TestActorStatus::Active,
    };

    let actor = remote.inner().new_actor(TestActor::new()).await.unwrap();
    let message = remote
        .create_message(
            &actor,
            msg.clone(),
        )
        .await
        .unwrap();

    assert_eq!(message.actor_id, actor.id);
    assert_eq!(message.handler_type, test_set_status);
    assert_eq!(message.message, msg);

    forget(remote);
}
