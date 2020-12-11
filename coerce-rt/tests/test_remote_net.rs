use coerce_rt::actor::context::ActorSystem;
use coerce_rt::remote::codec::json::JsonCodec;
use coerce_rt::remote::net::client::RemoteClient;
use coerce_rt::remote::net::server::RemoteServer;
use coerce_rt::remote::system::builder::RemoteActorHandlerBuilder;
use coerce_rt::remote::system::RemoteActorSystem;

use coerce_rt::remote::RemoteActorRef;

use util::*;
use uuid::Uuid;

pub mod util;

#[macro_use]
extern crate serde;
extern crate serde_json;

extern crate chrono;

#[macro_use]
extern crate async_trait;

#[tokio::test]
pub async fn test_remote_server_client_connection() {
    util::create_trace_logger();

    let mut system = ActorSystem::new();
    let actor = system.new_tracked_actor(TestActor::new()).await.unwrap();

    let remote = RemoteActorSystem::builder()
        .with_actor_system(system)
        .with_handlers(build_handlers)
        .build()
        .await;

    let mut remote_2 = RemoteActorSystem::builder()
        .with_actor_system(ActorSystem::new())
        .with_handlers(build_handlers)
        .build()
        .await;

    let mut server = RemoteServer::new(JsonCodec::new());
    match server.start("localhost:30101".to_string(), remote).await {
        Ok(_) => log::trace!("started!"),
        Err(_e) => panic!("failed to start server"),
    }

    let node_id = Uuid::new_v4();
    remote_2
        .register_client(
            node_id,
            RemoteClient::connect(
                "localhost:30101".to_string(),
                remote_2.clone(),
                JsonCodec::new(),
                None,
            )
            .await
            .unwrap(),
        )
        .await;

    let mut remote_actor = RemoteActorRef::<TestActor>::new(actor.id.clone(), node_id, remote_2);

    for _i in 1..=10 as i32 {
        let _initial_status = remote_actor.send(GetStatusRequest()).await;
    }

    let initial_status = remote_actor.send(GetStatusRequest()).await;
    let _ = remote_actor
        .send(SetStatusRequest {
            status: TestActorStatus::Active,
        })
        .await;

    let current_status = remote_actor.send(GetStatusRequest()).await;

    let _ = remote_actor
        .send(SetStatusRequest {
            status: TestActorStatus::Inactive,
        })
        .await;

    let inactive_status = remote_actor.send(GetStatusRequest()).await;

    assert_eq!(initial_status, Ok(GetStatusResponse::None));

    assert_eq!(
        inactive_status,
        Ok(GetStatusResponse::Ok(TestActorStatus::Inactive))
    );

    assert_eq!(
        current_status,
        Ok(GetStatusResponse::Ok(TestActorStatus::Active))
    );
}

fn build_handlers(handlers: &mut RemoteActorHandlerBuilder) -> &mut RemoteActorHandlerBuilder {
    handlers
        .with_handler::<TestActor, SetStatusRequest>("TestActor.SetStatusRequest")
        .with_handler::<TestActor, GetStatusRequest>("TestActor.GetStatusRequest")
        .with_handler::<EchoActor, GetCounterRequest>("EchoActor.GetCounterRequest")
}
