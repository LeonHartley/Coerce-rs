use crate::actor::message::{Envelope, Handler, Message, MessageWrapErr};
use crate::actor::{Actor, ActorId, ActorRefErr};
use crate::remote::actor::RemoteResponse;
use crate::remote::net::message::SessionEvent;
use crate::remote::net::proto::network::MessageRequest;
use crate::remote::system::{NodeId, RemoteActorSystem};

use std::fmt::{Debug, Formatter};

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
        // let message_type = Msg::type_name();
        // let actor_type = A::type_name();
        // let span = tracing::trace_span!("RemoteActorRef::notify", actor_type, message_type);
        // let _enter = span.enter();

        let id = Uuid::new_v4();

        let request = self.create_request(msg, String::new(), id, false)?;
        self.system.notify_node(self.node_id, request).await;

        Ok(())
    }

    pub async fn send<Msg: Message>(&self, msg: Envelope<Msg>) -> Result<Msg::Result, ActorRefErr>
    where
        A: Handler<Msg>,
        Msg: 'static + Send + Sync,
        <Msg as Message>::Result: 'static + Send + Sync,
    {
        let id = Uuid::new_v4();

        let (res_tx, res_rx) = oneshot::channel();
        self.system.push_request(id, res_tx);

        let event = self.create_request(msg, String::new(), id, true)?;

        // TODO: we could make this fail fast if the node is known to be terminated?
        self.system.notify_node(self.node_id, event).await;
        match res_rx.await {
            Ok(RemoteResponse::Ok(res)) => match Msg::read_remote_result(res) {
                Ok(res) => Ok(res),
                Err(e) => {
                    error!("failed to decode result");
                    Err(ActorRefErr::Deserialisation(e))
                }
            },
            Err(e) => {
                error!("failed to receive result, e={}", e);
                Err(ActorRefErr::ResultChannelClosed)
            }
            Ok(RemoteResponse::Err(e)) => Err(e),
        }
    }

    fn create_request<Msg: Message>(
        &self,
        msg: Envelope<Msg>,
        trace_id: String,
        id: Uuid,
        requires_response: bool,
    ) -> Result<SessionEvent, ActorRefErr>
    where
        Msg: 'static + Send + Sync,
    {
        let message = match msg {
            Envelope::Remote(b) => b,
            _ => return Err(ActorRefErr::Serialisation(MessageWrapErr::NotTransmittable)),
        };

        self.system.create_header::<A, Msg>(&self.id).map(|header| {
            let handler_type = header.handler_type;
            let actor_id = header.actor_id.to_string();
            let origin_node_id = self.system.node_id();
            SessionEvent::NotifyActor(MessageRequest {
                message_id: id.to_string(),
                handler_type,
                actor_id,
                trace_id,
                message,
                requires_response,
                origin_node_id,
                ..Default::default()
            })
        })
    }
}

impl<A: Actor> Debug for RemoteActorRef<A> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(&format!("RemoteActorRef<{}>", A::type_name()))
            .field("actor_id", &self.id)
            .field("node_id", &self.node_id)
            .finish()
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
