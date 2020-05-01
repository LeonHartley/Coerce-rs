pub mod util;

#[macro_use]
extern crate serde;
extern crate serde_json;

extern crate chrono;

#[macro_use]
extern crate async_trait;

#[macro_use]
extern crate coerce_macros;

use coerce_remote::cluster::client::RemoteClusterClient;
use coerce_remote::context::RemoteActorContext;
use coerce_rt::actor::context::ActorContext;
use util::*;

#[coerce_test]
pub async fn test_remote_cluster_client_builder() {
    util::create_trace_logger();

    let mut context = ActorContext::new();
    let _actor = context.new_tracked_actor(TestActor::new()).await.unwrap();
    let remote = RemoteActorContext::builder()
        .with_actor_context(context)
        .with_handlers(|builder| builder.with_actor::<TestActor>("TestActor"))
        .build()
        .await;

    let mut client = remote.cluster_client().build();
    let actor = client.get_actor::<TestActor>(format!("leon")).await;

    assert_eq!(actor.is_some(), false);
}
