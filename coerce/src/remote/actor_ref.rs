use crate::actor::message::{Envelope, Handler, Message};
use crate::actor::ActorRefErr::ActorUnavailable;
use crate::actor::{Actor, ActorId, ActorRefErr};
use crate::remote::actor::RemoteResponse;
use crate::remote::net::message::SessionEvent;
use crate::remote::net::proto::network::MessageRequest;
use crate::remote::system::{NodeId, RemoteActorSystem};
use crate::remote::tracing::extract_trace_identifier;

use std::marker::PhantomData;

use tokio::sync::oneshot;

use uuid::Uuid;

pub struct RemoteActorRef<A: Actor>
where
    A: 'static + Sync + Send,
{
    id: ActorId,
    system: RemoteActorSystem,
    node_id: NodeId,
    _a: PhantomData<A>,
}

pub struct RemoteMessageHeader {
    pub actor_id: ActorId,
    pub handler_type: String,
}

impl<A: Actor> RemoteActorRef<A>
where
    A: 'static + Sync + Send,
{
    pub fn new(id: ActorId, node_id: NodeId, system: RemoteActorSystem) -> RemoteActorRef<A> {
        RemoteActorRef {
            id,
            system,
            node_id,
            _a: PhantomData,
        }
    }

    pub fn actor_id(&self) -> &ActorId {
        &self.id
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub async fn notify<Msg: Message>(&self, msg: Envelope<Msg>) -> Result<(), ActorRefErr>
    where
        A: Handler<Msg>,
        Msg: 'static + Send + Sync,
    {
        let message_type = Msg::type_name();
        let actor_type = A::type_name();
        let span = tracing::trace_span!("RemoteActorRef::notify", actor_type, message_type);
        let _enter = span.enter();

        let id = Uuid::new_v4();
        let request = self.create_request(msg, extract_trace_identifier(&span), id);

        match request {
            Some(request) => {
                self.system.send_message(self.node_id, request).await;
                Ok(())
            }

            None => Err(ActorRefErr::ActorUnavailable),
        }
    }

    pub async fn send<Msg: Message>(&self, msg: Envelope<Msg>) -> Result<Msg::Result, ActorRefErr>
    where
        A: Handler<Msg>,
        Msg: 'static + Send + Sync,
        <Msg as Message>::Result: 'static + Send + Sync,
    {
        let message_type = Msg::type_name();
        let actor_type = A::type_name();
        let span = tracing::trace_span!("RemoteActorRef::send", actor_type, message_type);
        let _enter = span.enter();

        let id = Uuid::new_v4();
        let event = self.create_request(msg, extract_trace_identifier(&span), id);

        let (res_tx, res_rx) = oneshot::channel();
        self.system.push_request(id, res_tx);

        match event {
            Some(event) => {
                self.system.send_message(self.node_id, event).await;
                match res_rx.await {
                    Ok(RemoteResponse::Ok(res)) => match Msg::read_remote_result(res) {
                        Ok(res) => Ok(res),
                        Err(e) => {
                            error!(target: "RemoteActorRef", "failed to decode result");
                            Err(ActorRefErr::Deserialisation(e))
                        }
                    },
                    Err(e) => {
                        error!(target: "RemoteActorRef", "failed to receive result, e={}", e);
                        Err(ActorRefErr::ResultChannelClosed)
                    }
                    Ok(RemoteResponse::Err(_e)) => {
                        // TODO: return custom error
                        Err(ActorUnavailable)
                    }
                }
            }
            None => {
                error!(target: "RemoteActorRef", "no handler registered actor_type={}, message_type={}", &actor_type, message_type);
                Err(ActorRefErr::NotSupported {
                    actor_id: self.id.clone(),
                    message_type: message_type.to_string(),
                    actor_type: actor_type.to_string(),
                })
            }
        }
    }

    fn create_request<Msg: Message>(
        &self,
        msg: Envelope<Msg>,
        trace_id: String,
        id: Uuid,
    ) -> Option<SessionEvent>
    where
        Msg: 'static + Send + Sync,
    {
        let message_bytes = match msg {
            Envelope::Remote(b) => b,
            _ => return None,
        };

        let event = self.system.create_header::<A, Msg>(&self.id).map(|header| {
            SessionEvent::NotifyActor(MessageRequest {
                message_id: id.to_string(),
                handler_type: header.handler_type,
                actor_id: header.actor_id,
                trace_id,
                message: message_bytes,
                requires_response: false,
                ..MessageRequest::default()
            })
        });

        event
    }
}

impl<A: Actor> Clone for RemoteActorRef<A>
where
    A: 'static + Sync + Send,
{
    fn clone(&self) -> Self {
        RemoteActorRef::new(self.id.clone(), self.node_id, self.system.clone())
    }
}
