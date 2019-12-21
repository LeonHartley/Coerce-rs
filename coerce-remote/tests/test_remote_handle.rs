use crate::util::create_trace_logger;

use coerce_rt::actor::context::ActorContext;
use coerce_rt::actor::message::Handler;

use coerce_remote::context::RemoteActorContext;

use std::mem::forget;

use coerce_rt::actor::scheduler::ActorType::Tracked;

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

    let mut remote = RemoteActorContext::builder()
        .with_actor_context(ActorContext::new())
        .with_handlers(|handlers| {
            handlers
                .with_handler::<TestActor, SetStatusRequest>("TestActor.SetStatusRequest")
                .with_handler::<TestActor, GetStatusRequest>("TestActor.GetStatusRequest")
                .with_handler::<EchoActor, GetCounterRequest>("EchoActor.GetCounterRequest")
        })
        .build()
        .await;

    assert_eq!(
        remote.handler_name::<EchoActor, GetCounterRequest>().await,
        Some(echo_get_counter)
    );
    assert_eq!(
        remote.handler_name::<TestActor, SetStatusRequest>().await,
        Some(test_set_status)
    );
    assert_eq!(
        remote.handler_name::<TestActor, GetStatusRequest>().await,
        Some(test_get_status)
    );

    forget(remote);
}

#[tokio::test]
pub async fn test_remote_handle_from_json() {
    create_trace_logger();

    let mut ctx = ActorContext::new();
    let mut actor = ctx.new_actor(TestActor::new(), Tracked).await.unwrap();

    let mut remote = RemoteActorContext::builder()
        .with_actor_context(ctx)
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
        .handle(
            "TestActor.SetStatusRequest".to_string(),
            actor.id,
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

    forget(remote);
}
