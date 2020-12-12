use crate::actor::message::{Envelope, EnvelopeType, Handler, Message, RemoteMessage};
use crate::actor::ActorRefErr::ActorUnavailable;
use crate::actor::{Actor, ActorId, ActorRefErr, LocalActorRef};
use crate::remote::actor::RemoteResponse;
use crate::remote::net::message::{MessageRequest, SessionEvent};
use crate::remote::system::RemoteActorSystem;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::marker::PhantomData;
use uuid::Uuid;

pub struct RemoteActorRef<A: Actor>
where
    A: 'static + Sync + Send,
{
    id: ActorId,
    context: RemoteActorSystem,
    node_id: Uuid,
    _a: PhantomData<A>,
}

impl<A: Actor> RemoteActorRef<A>
where
    A: 'static + Sync + Send,
{
    pub fn new(id: ActorId, node_id: Uuid, context: RemoteActorSystem) -> RemoteActorRef<A> {
        RemoteActorRef {
            id,
            context,
            node_id,
            _a: PhantomData,
        }
    }

    pub async fn send<Msg: Message>(
        &mut self,
        msg: Envelope<Msg>,
    ) -> Result<Msg::Result, ActorRefErr>
    where
        A: Handler<Msg>,
        Msg: 'static + Send + Sync,
        <Msg as Message>::Result: 'static + Send + Sync,
    {
        let id = Uuid::new_v4();
        let (res_tx, res_rx) = tokio::sync::oneshot::channel();

        let message_bytes = match msg {
            Envelope::Remote(b) => b,
            _ => return Err(ActorRefErr::ActorUnavailable),
        };

        let event = self
            .context
            .create_header::<A, Msg>(&self.id)
            .map(|header| {
                SessionEvent::Message(MessageRequest {
                    id,
                    handler_type: header.handler_type,
                    actor: header.actor_id,
                    message: message_bytes,
                })
            });

        self.context.push_request(id, res_tx).await;

        match event {
            Some(event) => {
                self.context.send_message(self.node_id, event).await;
                match res_rx.await {
                    Ok(RemoteResponse::Ok(res)) => match Msg::read_remote_result(res) {
                        Ok(res) => Ok(res),
                        Err(_) => {
                            error!(target: "RemoteActorRef", "failed to decode result");
                            Err(ActorUnavailable)
                        }
                    },
                    _ => {
                        error!(target: "RemoteActorRef", "failed to receive result");
                        Err(ActorUnavailable)
                    }
                }
            }
            None => {
                error!(target: "RemoteActorRef", "no handler returned");
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
        RemoteActorRef::new(self.id.clone(), self.node_id, self.context.clone())
    }
}
