use crate::actor::message::{Envelope, Handler, Message};
use crate::actor::ActorRefErr::ActorUnavailable;
use crate::actor::{Actor, ActorId, ActorRefErr};
use crate::remote::actor::RemoteResponse;
use crate::remote::net::message::SessionEvent;
use crate::remote::net::proto::protocol::MessageRequest;
use crate::remote::system::RemoteActorSystem;
use crate::remote::tracing::extract_trace_identifier;

use std::marker::PhantomData;

use tokio::sync::oneshot::error::RecvError;
use uuid::Uuid;

pub struct RemoteActorRef<A: Actor>
where
    A: 'static + Sync + Send,
{
    id: ActorId,
    system: RemoteActorSystem,
    node_id: Uuid,
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
    pub fn new(id: ActorId, node_id: Uuid, system: RemoteActorSystem) -> RemoteActorRef<A> {
        RemoteActorRef {
            id,
            system,
            node_id,
            _a: PhantomData,
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
        let (res_tx, res_rx) = tokio::sync::oneshot::channel();
        let message_bytes = match msg {
            Envelope::Remote(b) => b,
            _ => return Err(ActorRefErr::ActorUnavailable),
        };

        let trace_id = extract_trace_identifier(&span);
        let event = self.system.create_header::<A, Msg>(&self.id).map(|header| {
            SessionEvent::NotifyActor(MessageRequest {
                message_id: id.to_string(),
                handler_type: header.handler_type,
                actor_id: header.actor_id,
                trace_id,
                message: message_bytes,
                ..MessageRequest::default()
            })
        });

        self.system.push_request(id, res_tx).await;

        match event {
            Some(event) => {
                self.system.send_message(self.node_id, event).await;
                match res_rx.await {
                    Ok(RemoteResponse::Ok(res)) => match Msg::read_remote_result(res) {
                        Ok(res) => Ok(res),
                        Err(_) => {
                            error!(self.system.actor_system().log(), "failed to decode result");
                            Err(ActorUnavailable)
                        }
                    },
                    Err(e) => {
                        error!(
                            self.system.actor_system().log(),
                            "failed to receive result, e={}", e
                        );
                        Err(ActorUnavailable)
                    }
                    Ok(RemoteResponse::Err(e)) => {
                        // TODO: return custom error
                        Err(ActorUnavailable)
                    }
                    _ => Err(ActorUnavailable),
                }
            }
            None => {
                error!(self.system.actor_system().log(), "no handler returned");
                // TODO: add more errors
                Err(ActorUnavailable)
            }
        }
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
