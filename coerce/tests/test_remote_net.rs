// use coerce::actor::system::ActorSystem;
// use coerce::remote::net::client::{ClientType, RemoteClient};
// use coerce::remote::net::server::RemoteServer;
// use coerce::remote::system::builder::RemoteActorHandlerBuilder;
// use coerce::remote::system::RemoteActorSystem;
// use coerce::remote::RemoteActorRef;
//
// use coerce::actor::ActorRef;
//
// use util::*;
//
// pub mod util;
//
// #[macro_use]
// extern crate serde;
//
// #[macro_use]
// extern crate async_trait;
//
// #[tokio::test]
// pub async fn test_remote_server_client_connection() {
//     // let (tracer, _uninstall) = opentelemetry_jaeger::new_pipeline()
//     //     .with_service_name("coerce")
//     //     .install()
//     //     .expect("jaeger");
//     // let opentelemetry = tracing_opentelemetry::layer().with_tracer(tracer);
//     // tracing_subscriber::registry()
//     //     .with(opentelemetry)
//     //     .try_init()
//     //     .expect("tracing init");
//
//     let system = ActorSystem::new();
//     let actor = system.new_tracked_actor(TestActor::new()).await.unwrap();
//
//     let remote = RemoteActorSystem::builder()
//         .with_actor_system(system)
//         .with_handlers(build_handlers)
//         .build()
//         .await;
//
//     let remote_2 = RemoteActorSystem::builder()
//         .with_actor_system(ActorSystem::new())
//         .with_handlers(build_handlers)
//         .build()
//         .await;
//
//     let mut server = RemoteServer::new();
//     match server.start("localhost:30101".to_string(), remote).await {
//         Ok(_) => tracing::trace!("started!"),
//         Err(_e) => panic!("failed to start server"),
//     }
//
//     let node_id = 1;
//     remote_2
//         .register_client(
//             node_id,
//             RemoteClient::connect(
//                 "localhost:30101".to_string(),
//                 Some(node_id),
//                 remote_2.clone(),
//                 None,
//                 ClientType::Worker,
//             )
//             .await
//             .unwrap(),
//         )
//         .await;
//
//     let remote_actor: ActorRef<TestActor> =
//         RemoteActorRef::<TestActor>::new(actor.id.clone(), node_id, remote_2).into();
//
//     for _i in 1..=10 as i32 {
//         let _initial_status = remote_actor.send(GetStatusRequest).await;
//     }
//
//     let initial_status = remote_actor.send(GetStatusRequest).await;
//     let _ = remote_actor
//         .send(SetStatusRequest {
//             status: TestActorStatus::Active,
//         })
//         .await;
//
//     let current_status = remote_actor.send(GetStatusRequest).await;
//
//     let _ = remote_actor
//         .send(SetStatusRequest {
//             status: TestActorStatus::Inactive,
//         })
//         .await;
//
//     let inactive_status = remote_actor.send(GetStatusRequest).await;
//
//     assert_eq!(initial_status, Ok(GetStatusResponse::None));
//
//     assert_eq!(
//         inactive_status,
//         Ok(GetStatusResponse::Ok(TestActorStatus::Inactive))
//     );
//
//     assert_eq!(
//         current_status,
//         Ok(GetStatusResponse::Ok(TestActorStatus::Active))
//     );
// }
//
// fn build_handlers(handlers: &mut RemoteActorHandlerBuilder) -> &mut RemoteActorHandlerBuilder {
//     handlers
//         .with_handler::<TestActor, SetStatusRequest>("TestActor.SetStatusRequest")
//         .with_handler::<TestActor, GetStatusRequest>("TestActor.GetStatusRequest")
//         .with_handler::<EchoActor, GetCounterRequest>("EchoActor.GetCounterRequest")
// }
