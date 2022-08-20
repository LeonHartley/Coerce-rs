use crate::util::{GetStatusRequest, SetStatusRequest, TestActor, TestActorStatus};
use coerce::actor::context::ActorContext;
use coerce::actor::lifecycle::Stop;
use coerce::actor::message::{Handler, Message, MessageWrapErr};
use coerce::actor::scheduler::ActorType::Tracked;
use coerce::actor::system::ActorSystem;
use coerce::actor::{ActorRef, ActorRefErr, ToActorId};
use coerce::remote::system::builder::{RemoteActorSystemBuilder, RemoteSystemConfigBuilder};
use coerce::remote::system::RemoteActorSystem;
use coerce::remote::RemoteActorRef;

#[macro_use]
extern crate async_trait;

#[macro_use]
extern crate serde;

mod util;

struct NotSerialisable;

impl Message for NotSerialisable {
    type Result = ();
}

#[async_trait]
impl Handler<NotSerialisable> for TestActor {
    async fn handle(&mut self, _: NotSerialisable, _: &mut ActorContext) {}
}

fn remote_handlers(builder: &mut RemoteSystemConfigBuilder) -> &mut RemoteSystemConfigBuilder {
    builder.with_handler::<TestActor, GetStatusRequest>("TestActor.GetStatusRequest")
}

#[tokio::test]
pub async fn test_remote_actor_err() {
    util::create_trace_logger();
    let node_1 = util::create_cluster_node(1, "localhost:30101", None, |handlers| {
        remote_handlers(handlers)
    })
    .await;

    let node_2 =
        util::create_cluster_node(2, "localhost:30201", Some("localhost:30101"), |handlers| {
            remote_handlers(handlers)
        })
        .await;

    let actor_id = "test_actor".to_actor_id();
    let local_ref = node_1
        .actor_system()
        .new_actor(actor_id.clone(), TestActor::new(), Tracked)
        .await
        .expect("create TestActor on node=1");

    let _ = local_ref.notify(SetStatusRequest {
        status: TestActorStatus::Active,
    });

    let actor_ref = ActorRef::from(RemoteActorRef::<TestActor>::new(
        actor_id.clone(),
        node_1.node_id(),
        node_2.clone(),
    ));

    let status = actor_ref.send(GetStatusRequest).await;
    assert!(status.is_ok());

    let _ = local_ref.stop().await;

    let status = actor_ref.send(GetStatusRequest).await;
    assert_eq!(status.unwrap_err(), ActorRefErr::NotFound(actor_id.clone()));

    let status = actor_ref.send(NotSerialisable).await;
    assert_eq!(
        status.unwrap_err(),
        ActorRefErr::Serialisation(MessageWrapErr::NotTransmittable)
    );

    let status = actor_ref
        .send(SetStatusRequest {
            status: TestActorStatus::Inactive,
        })
        .await;

    assert_eq!(
        status.unwrap_err(),
        ActorRefErr::NotSupported {
            actor_id,
            message_type: "test_remote_actor_err::util::SetStatusRequest".to_string(),
            actor_type: "test_remote_actor_err::util::TestActor".to_string()
        }
    );
}
