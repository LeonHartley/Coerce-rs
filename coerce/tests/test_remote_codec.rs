use coerce::actor::system::ActorSystem;
use coerce::remote::system::RemoteActorSystem;

use coerce::remote::net::proto::protocol::CreateActor;
use util::*;

pub mod util;

#[macro_use]
extern crate serde;

#[macro_use]
extern crate async_trait;

#[tokio::test]
pub async fn test_remote_create_message() {
    util::create_trace_logger();

    let _createActor = CreateActor::new();

    let mut remote = RemoteActorSystem::builder()
        .with_actor_system(ActorSystem::new())
        .with_handlers(move |handlers| {
            handlers.with_handler::<TestActor, SetStatusRequest>("TestActor.SetStatusRequest")
        })
        .build()
        .await;

    let _msg = SetStatusRequest {
        status: TestActorStatus::Active,
    };

    let actor = remote
        .inner()
        .new_anon_actor(TestActor::new())
        .await
        .unwrap();

    let message = remote
        .create_header::<TestActor, SetStatusRequest>(&actor.id)
        .unwrap();

    assert_eq!(message.actor_id, actor.id);
    assert_eq!(
        message.handler_type,
        "TestActor.SetStatusRequest".to_string()
    );
}
