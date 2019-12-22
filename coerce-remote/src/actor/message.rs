use crate::actor::{BoxedHandler, RemoteRequest};
use crate::handler::RemoteActorMessageMarker;
use coerce_rt::actor::message::Message;
use coerce_rt::actor::Actor;
use uuid::Uuid;

pub struct HandlerName<A: Actor, M: Message>
where
    A: 'static + Send + Sync,
    M: 'static + Send + Sync,
    M::Result: 'static + Sync + Send,
{
    marker: RemoteActorMessageMarker<A, M>,
}

impl<A: Actor, M: Message> HandlerName<A, M>
where
    A: 'static + Send + Sync,
    M: 'static + Send + Sync,
    M::Result: 'static + Send + Sync,
{
    pub fn new() -> HandlerName<A, M> {
        HandlerName {
            marker: RemoteActorMessageMarker::new(),
        }
    }
}

impl<A: Actor, M: Message> Message for HandlerName<A, M>
where
    A: 'static + Send + Sync,
    M: 'static + Send + Sync,
    M::Result: 'static + Send + Sync,
{
    type Result = Option<String>;
}

pub struct GetHandler(pub String);

impl Message for GetHandler {
    type Result = Option<BoxedHandler>;
}

pub struct PushRequest(pub Uuid, pub RemoteRequest);

impl Message for PushRequest {
    type Result = ();
}

pub struct PopRequest(pub Uuid);

impl Message for PopRequest {
    type Result = Option<RemoteRequest>;
}
