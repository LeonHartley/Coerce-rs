use crate::actor::context::ActorContext;
use crate::actor::message::Message;
use crate::actor::{
    new_actor_id, Actor, ActorFactory, ActorId, ActorRecipe, ActorRef, CoreActorRef, IntoActorId,
};
use crate::remote::actor::message::{GetActorNode, RegisterActor};
use crate::remote::handler::send_proto_result;
use crate::remote::net::message::SessionEvent;
use crate::remote::net::proto::network::{ActorAddress, CreateActorEvent};
use crate::remote::system::{NodeId, NodeRpcErr, RemoteActorSystem};
use crate::remote::{RemoteActorRef, RemoteMessageHeader};
use protobuf::well_known_types::wrappers::UInt64Value;
use protobuf::{Message as ProtoMessage, MessageField};
use tokio::sync::oneshot;
use uuid::Uuid;

#[derive(Debug, Eq, PartialEq)]
pub enum RemoteActorErr {
    ActorUnavailable,
    ActorExists,
    RecipeSerializationErr,
    MessageSerializationErr,
    ResultSerializationErr,
    ActorNotSupported,
    NodeErr(NodeRpcErr),
}

impl RemoteActorSystem {
    pub fn register_actor(&self, actor_id: ActorId, node_id: Option<NodeId>) {
        let _ = self
            .inner
            .registry_ref
            .notify(RegisterActor::new(actor_id, node_id));
    }

    pub async fn actor_ref<A: Actor>(&self, actor_id: ActorId) -> Option<ActorRef<A>> {
        // let actor_type_name = A::type_name();
        // let span = tracing::trace_span!(
        //     "RemoteActorSystem::actor_ref",
        //     actor_id = actor_id.as_str(),
        //     actor_type_name
        // );
        // let _enter = span.enter();

        match self.locate_actor_node(actor_id.clone()).await {
            Some(node_id) => {
                if node_id == self.inner.node_id {
                    self.inner
                        .inner
                        .get_tracked_actor(actor_id)
                        .await
                        .map(ActorRef::from)
                } else {
                    Some(ActorRef::from(RemoteActorRef::new(
                        actor_id,
                        node_id,
                        self.clone(),
                    )))
                }
            }
            None => None,
        }
    }

    pub async fn locate_actor_node(&self, actor_id: ActorId) -> Option<NodeId> {
        // let span = tracing::trace_span!(
        //     "RemoteActorSystem::locate_actor_node",
        //     actor_id = actor_id.as_str()
        // );
        // let _enter = span.enter();

        trace!(
            "locating actor node (current_node={}, actor_id={})",
            self.node_tag(),
            &actor_id
        );
        let (tx, rx) = oneshot::channel();
        match self
            .inner
            .registry_ref
            .send(GetActorNode {
                actor_id: actor_id.clone(),
                sender: tx,
            })
            .await
        {
            Ok(_) => {
                // TODO: configurable timeouts (?)
                match tokio::time::timeout(tokio::time::Duration::from_secs(5), rx).await {
                    Ok(Ok(res)) => {
                        trace!(
                            "received actor node (current_node={}, actor_id={}, actor_node={:?})",
                            self.node_tag(),
                            &actor_id,
                            &res
                        );
                        res
                    }
                    Ok(Err(e)) => {
                        error!("error receiving result, {}", e);
                        None
                    }
                    Err(e) => {
                        error!("error receiving result, {}", e);
                        None
                    }
                }
            }
            Err(_e) => {
                error!("error sending message");
                None
            }
        }
    }

    pub async fn deploy_actor<F: ActorFactory>(
        &self,
        id: Option<ActorId>,
        recipe: F::Recipe,
        node: Option<NodeId>,
    ) -> Result<ActorRef<F::Actor>, RemoteActorErr> {
        let self_id = self.node_id();
        let id = id.map_or_else(new_actor_id, |id| id);
        let node = node.map_or_else(|| self_id, |n| n);
        let actor_type: String = F::Actor::type_name().into();

        let recipe = recipe.write_to_bytes();
        if recipe.is_none() {
            return Err(RemoteActorErr::RecipeSerializationErr);
        }

        let recipe = recipe.unwrap();
        let message_id = Uuid::new_v4();

        let mut actor_addr = None;
        if node == self_id {
            let local_create = self
                .handle_create_actor(Some(id.clone()), actor_type, recipe)
                .await;

            if local_create.is_ok() {
                actor_addr = Some(ActorAddress::parse_from_bytes(&local_create.unwrap()).unwrap());
            } else {
                return Err(local_create.unwrap_err());
            }
        } else {
            let message = CreateActorEvent {
                actor_type,
                message_id: message_id.to_string(),
                actor_id: id.to_string(),
                recipe,
                ..Default::default()
            };

            match self
                .node_rpc_proto::<ActorAddress>(
                    message_id,
                    SessionEvent::CreateActor(message),
                    node,
                )
                .await
            {
                Ok(address) => actor_addr = Some(address),
                Err(e) => return Err(RemoteActorErr::NodeErr(e)),
            }
        }

        match actor_addr {
            Some(actor_address) => {
                let actor_ref = RemoteActorRef::<F::Actor>::new(
                    actor_address.actor_id.into_actor_id(),
                    actor_address.node_id.value,
                    self.clone(),
                );

                Ok(ActorRef::from(actor_ref))
            }
            None => Err(RemoteActorErr::ActorUnavailable),
        }
    }

    pub async fn handle_create_actor(
        &self,
        actor_id: Option<ActorId>,
        actor_type: String,
        raw_recipe: Vec<u8>,
    ) -> Result<Vec<u8>, RemoteActorErr> {
        let (tx, rx) = oneshot::channel();

        if let Some(actor_id) = &actor_id {
            if let Some(node_id) = self.locate_actor_node(actor_id.clone()).await {
                warn!("actor {} already exists on node: {}", &actor_id, &node_id);
                return Err(RemoteActorErr::ActorExists);
            }
        }

        let actor_id = actor_id.map_or_else(new_actor_id, |id| id);

        trace!("creating actor (actor_id={})", &actor_id);
        let handler = self.inner.config.actor_handler(&actor_type);

        if let Some(handler) = handler {
            let actor_ref = handler
                .create(
                    Some(actor_id.clone()),
                    &raw_recipe,
                    None,
                    Some(self.actor_system()),
                )
                .await;

            match actor_ref {
                Ok(actor_ref) => {
                    let result = ActorAddress {
                        actor_id: actor_ref.actor_id().to_string(),
                        node_id: MessageField::some(UInt64Value::from(self.node_id())),
                        ..ActorAddress::default()
                    };

                    trace!("sending created actor ref");
                    send_proto_result(result, tx);
                }
                Err(_) => return Err(RemoteActorErr::ActorUnavailable),
            }
        } else {
            trace!("No handler found with the type: {}", &actor_type);
            return Err(RemoteActorErr::ActorNotSupported);
        }

        trace!("waiting for actor to start");

        match rx.await {
            Ok(res) => {
                trace!("actor started (actor_id={})", &actor_id);

                Ok(res)
            }
            Err(_e) => Err(RemoteActorErr::ActorUnavailable),
        }
    }

    pub fn handler_name<A: Actor, M: Message>(&self) -> Option<String> {
        self.inner.config.handler_name::<A, M>()
    }

    pub fn create_header<A: Actor, M: Message>(&self, id: &ActorId) -> Option<RemoteMessageHeader> {
        self.handler_name::<A, M>()
            .map(|handler_type| RemoteMessageHeader {
                actor_id: id.clone(),
                handler_type,
            })
    }
}
