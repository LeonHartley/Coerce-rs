use crate::util::create_trace_logger;
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
pub async fn test_remote_handler_types() {
    let echo_get_counter = "EchoActor.GetCounterRequest".to_string();
    let test_get_status = "TestActor.GetStatusRequest".to_string();
    let test_set_status = "TestActor.SetStatusRequest".to_string();

    let mut remote = RemoteActorSystem::builder()
        .with_actor_system(ActorSystem::new())
        .with_handlers(|handlers| {
            handlers
                .with_handler::<TestActor, SetStatusRequest>("TestActor.SetStatusRequest")
                .with_handler::<TestActor, GetStatusRequest>("TestActor.GetStatusRequest")
                .with_handler::<EchoActor, GetCounterRequest>("EchoActor.GetCounterRequest")
        })
        .build()
        .await;

    assert_eq!(
        remote.handler_name::<EchoActor, GetCounterRequest>(),
        Some(echo_get_counter)
    );
    assert_eq!(
        remote.handler_name::<TestActor, SetStatusRequest>(),
        Some(test_set_status)
    );
    assert_eq!(
        remote.handler_name::<TestActor, GetStatusRequest>(),
        Some(test_get_status)
    );
}

#[tokio::test]
pub async fn test_remote_handle_from_json() {
    create_trace_logger();

    let mut ctx = ActorSystem::new();
    let mut actor = ctx.new_tracked_actor(TestActor::new()).await.unwrap();

    let mut remote = RemoteActorSystem::builder()
        .with_actor_system(ctx)
        .with_handlers(|handlers| {
            handlers
                .with_handler::<TestActor, SetStatusRequest>("TestActor.SetStatusRequest")
                .with_handler::<TestActor, GetStatusRequest>("TestActor.GetStatusRequest")
                .with_handler::<EchoActor, GetCounterRequest>("EchoActor.GetCounterRequest")
        })
        .build()
        .await;

    let initial_status = actor.send(GetStatusRequest()).await;

    let res = remote
        .handle_message(
            "TestActor.SetStatusRequest".to_string(),
            actor.id.clone(),
            b"{\"status\": \"Active\"}",
        )
        .await;

    let current_status = actor.send(GetStatusRequest()).await;

    assert_eq!(res, Ok(b"\"Ok\"".to_vec()));

    assert_eq!(initial_status, Ok(GetStatusResponse::None));
    assert_eq!(
        current_status,
        Ok(GetStatusResponse::Ok(TestActorStatus::Active))
    );
}
