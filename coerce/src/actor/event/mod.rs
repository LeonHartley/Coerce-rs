///
/// Actor events provide the ability to monitor the creation of actors, what types of messages
/// they receive and monitor actors that are stopped.
///
/// This provides the ability to create pluggable actor monitoring without the need to manually
/// implement tracing inside each actor, it can be enabled at system level.
///
/// Applications such as dashboards can be created that can receive events as they happen, rather
/// than having to poll.
///
use crate::actor::BoxedActorRef;

pub mod dispatcher;

pub struct ActorStarted {
    timestamp: u64,
    actor_id: ActorId,
    actor_ref: BoxedActorRef,
}

pub struct ActorStopped {
    timestamp: u64,
    actor_ref: BoxedActorRef,
}

pub struct ActorReceived {
    timestamp: u64,
    actor_ref: BoxedActorRef,
    message_type: &'static str,
}

pub enum ActorEvent {
    Started(ActorStarted),
    Stopped(ActorStopped),
    Received(ActorReceived),
}

impl Message for Arc<ActorEvent> {
    type Result = ();
}