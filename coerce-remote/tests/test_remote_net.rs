use coerce_remote::codec::json::JsonCodec;
use coerce_remote::context::builder::RemoteActorHandlerBuilder;
use coerce_remote::context::RemoteActorContext;
use coerce_remote::net::client::RemoteClient;
use coerce_remote::net::server::RemoteServer;
use coerce_rt::actor::context::ActorContext;

use util::*;

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

    let remote = RemoteActorContext::builder()
        .with_actor_context(ActorContext::new())
        .with_handlers(build_handlers)
        .build()
        .await;

    let remote_2 = RemoteActorContext::builder()
        .with_actor_context(ActorContext::new())
        .with_handlers(build_handlers)
        .build()
        .await;

    let mut server = RemoteServer::new(JsonCodec::new());
    match server.start("localhost:30101".to_string(), remote).await {
        Ok(_) => log::trace!("started!"),
        Err(_e) => panic!("failed to start server"),
    }

    match RemoteClient::connect("localhost:30101".to_string(), remote_2, JsonCodec::new()).await {
        Ok(_client) => log::trace!("connected!"),
        Err(_e) => panic!("failed to connect to server"),
    }
}

fn build_handlers(handlers: &mut RemoteActorHandlerBuilder) -> &mut RemoteActorHandlerBuilder {
    handlers
        .with_handler::<TestActor, SetStatusRequest>("TestActor.SetStatusRequest")
        .with_handler::<TestActor, GetStatusRequest>("TestActor.GetStatusRequest")
        .with_handler::<EchoActor, GetCounterRequest>("EchoActor.GetCounterRequest")
}
