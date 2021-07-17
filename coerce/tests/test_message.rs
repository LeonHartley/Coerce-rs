use coerce::actor::context::ActorContext;
use coerce::actor::message::encoding::json::RemoteMessage;
use coerce::actor::message::{Envelope, EnvelopeType, Handler, Message, MessageWrapErr};
use coerce::actor::system::ActorSystem;
use coerce::actor::{Actor, IntoActor, Receiver};
use futures::FutureExt;
use util::*;

pub mod util;

#[macro_use]
extern crate serde;

#[macro_use]
extern crate async_trait;

#[tokio::test]
pub async fn test_actor_req_res() {
    let actor_ref = ActorSystem::new()
        .new_anon_actor(TestActor::new())
        .await
        .unwrap();

    let response = actor_ref.send(GetStatusRequest()).await;

    assert_eq!(response, Ok(GetStatusResponse::None));
}

#[tokio::test]
pub async fn test_actor_req_res_multiple_actors() {
    let ctx = ActorSystem::new();

    let test_ref = ctx.new_anon_actor(TestActor::new()).await.unwrap();
    let echo_ref = ctx.new_anon_actor(EchoActor::new()).await.unwrap();

    let test_res = test_ref.send(GetCounterRequest()).await;
    let echo_res = echo_ref.send(GetCounterRequest()).await;

    assert_eq!(test_res, Ok(42));
    assert_eq!(echo_res, Ok(42));
}

#[tokio::test]
pub async fn test_actor_req_res_mutation() {
    let actor_ref = ActorSystem::new()
        .new_anon_actor(TestActor::new())
        .await
        .unwrap();

    let initial_status = actor_ref.send(GetStatusRequest()).await;
    let _ = actor_ref
        .send(SetStatusRequest {
            status: TestActorStatus::Active,
        })
        .await;

    let current_status = actor_ref.send(GetStatusRequest()).await;

    let _ = actor_ref
        .send(SetStatusRequest {
            status: TestActorStatus::Inactive,
        })
        .await;

    let inactive_status = actor_ref.send(GetStatusRequest()).await;

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

#[tokio::test]
pub async fn test_actor_exec_mutation() {
    let actor_ref = ActorSystem::new()
        .new_anon_actor(TestActor::new())
        .await
        .unwrap();

    let initial_status = actor_ref.send(GetStatusRequest()).await;

    actor_ref
        .exec(|mut actor| {
            actor.status = Some(TestActorStatus::Active);
        })
        .await
        .expect("exec");

    let current_status = actor_ref.send(GetStatusRequest()).await;
    assert_eq!(initial_status, Ok(GetStatusResponse::None));
    assert_eq!(
        current_status,
        Ok(GetStatusResponse::Ok(TestActorStatus::Active))
    );
}

#[tokio::test]
pub async fn test_actor_exec_chain_mutation() {
    let actor_ref = ActorSystem::new()
        .new_anon_actor(TestActor::new())
        .await
        .unwrap();

    let _a = actor_ref
        .exec(|mut actor| {
            actor.counter = 1;
        })
        .await;

    let _ = actor_ref.exec(|mut actor| actor.counter = 2).await;

    let counter = actor_ref.exec(|actor| actor.counter).await;
    assert_eq!(counter, Ok(2));
}

#[tokio::test]
pub async fn test_actor_notify() {
    let actor_ref = ActorSystem::new()
        .new_anon_actor(TestActor::new())
        .await
        .unwrap();

    for i in 1..=25 as i32 {
        let _ = actor_ref.notify_exec(move |mut actor| actor.counter = i);
    }

    let counter = actor_ref.exec(|actor| actor.counter).await;
    assert_eq!(counter, Ok(25));
}

struct NewActor;
struct OtherActor;

impl Actor for NewActor {}
impl Actor for OtherActor {}

#[async_trait]
impl Handler<GetStatusRequest> for NewActor {
    async fn handle(
        &mut self,
        _message: GetStatusRequest,
        _ctx: &mut ActorContext,
    ) -> <GetStatusRequest as Message>::Result {
        GetStatusResponse::Ok(TestActorStatus::Active)
    }
}

#[async_trait]
impl Handler<GetStatusRequest> for OtherActor {
    async fn handle(
        &mut self,
        _message: GetStatusRequest,
        _ctx: &mut ActorContext,
    ) -> <GetStatusRequest as Message>::Result {
        GetStatusResponse::Ok(TestActorStatus::Active)
    }
}

#[tokio::test]
pub async fn test_actor_receiver() {
    let sys = ActorSystem::new();

    let actor_a = NewActor.into_actor(None, &sys).await.expect("NewActor");
    let actor_b = OtherActor
        .into_actor(None, &sys)
        .await
        .expect("OtherActor");

    let mut receivers: Vec<Receiver<GetStatusRequest>> = vec![actor_a.into(), actor_b.into()];

    let results =
        futures::future::join_all(receivers.iter_mut().map(|r| r.send(GetStatusRequest())))
            .await
            .into_iter()
            .map(|s| s.unwrap())
            .collect::<Vec<GetStatusResponse>>();

    assert_eq!(
        results,
        vec![
            GetStatusResponse::Ok(TestActorStatus::Active),
            GetStatusResponse::Ok(TestActorStatus::Active)
        ]
    )
}

#[derive(Serialize, Deserialize)]
struct MessageOne {}

struct NonSerializableMessage {}

impl Message for NonSerializableMessage {
    type Result = ();
}

impl RemoteMessage for MessageOne {
    type Result = ();
}

#[tokio::test]
pub async fn test_messages_into_remote_envelope() {
    let message = MessageOne {};
    let non_serializable_message = NonSerializableMessage {};

    let serializable_message = message.into_envelope(EnvelopeType::Remote);
    let non_serializable_envelope = non_serializable_message.into_envelope(EnvelopeType::Remote);

    let serialized_bytes = match serializable_message {
        Ok(Envelope::Remote(bytes)) => Some(bytes),
        _ => None,
    };

    assert!(serialized_bytes.is_some());
    assert!(non_serializable_envelope.is_err());
    assert_eq!(
        non_serializable_envelope.err(),
        Some(MessageWrapErr::NotTransmittable)
    );
}
