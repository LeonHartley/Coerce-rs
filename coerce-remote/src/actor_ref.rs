use crate::actor::RemoteResponse;
use crate::context::RemoteActorContext;
use crate::net::message::{MessageRequest, SessionEvent};
use coerce_rt::actor::message::{Handler, Message};
use coerce_rt::actor::ActorRefErr::ActorUnavailable;
use coerce_rt::actor::{Actor, ActorId, ActorRefErr};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::marker::PhantomData;
use uuid::Uuid;

pub struct RemoteActorRef<A: Actor>
where
    A: 'static + Sync + Send,
{
    id: ActorId,
    context: RemoteActorContext,
    node_id: Uuid,
    _a: PhantomData<A>,
}

impl<A: Actor> RemoteActorRef<A>
where
    A: 'static + Sync + Send,
{
    pub fn new(id: ActorId, node_id: Uuid, context: RemoteActorContext) -> RemoteActorRef<A> {
        RemoteActorRef {
            id,
            context,
            node_id,
            _a: PhantomData,
        }
    }

    pub async fn send<Msg: Message>(&mut self, msg: Msg) -> Result<Msg::Result, ActorRefErr>
    where
        Msg: 'static + Serialize + Send + Sync,
        A: Handler<Msg>,
        Msg::Result: DeserializeOwned + Send + Sync,
    {
        let id = Uuid::new_v4();
        let (res_tx, res_rx) = tokio::sync::oneshot::channel();

        let message = match serde_json::to_string(&msg) {
            Ok(e) => e,
            Err(_) => {
                error!(target: "RemoteActorRef", "error encoding message");
                return Err(ActorUnavailable);
            }
        };

        let event = self
            .context
            .create_message::<A, Msg>(&self.id, msg)
            .map(|m| {
                SessionEvent::Message(MessageRequest {
                    id,
                    handler_type: m.handler_type,
                    actor: m.actor_id,
                    message,
                })
            });

        self.context.push_request(id, res_tx).await;

        match event {
            Some(event) => {
                self.context.send_message(self.node_id, event).await;
                match res_rx.await {
                    Ok(RemoteResponse::Ok(res)) => {
                        match serde_json::from_slice::<Msg::Result>(res.as_slice()) {
                            Ok(res) => Ok(res),
                            Err(_) => {
                                error!(target: "RemoteActorRef", "failed to decode result");
                                Err(ActorUnavailable)
                            }
                        }
                    }
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
