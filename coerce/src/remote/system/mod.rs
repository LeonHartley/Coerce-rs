use crate::actor::message::Message;
use crate::actor::system::ActorSystem;
use crate::actor::{
    new_actor_id, Actor, ActorFactory, ActorId, ActorRecipe, ActorRef, LocalActorRef,
};
use crate::remote::actor::message::{
    ClientWrite, DeregisterClient, GetActorNode, GetNodes, PopRequest, PushRequest, RegisterActor,
    RegisterClient, RegisterNode, RegisterNodes,
};
use crate::remote::actor::{
    RemoteClientRegistry, RemoteHandler, RemoteHandlerTypes, RemoteRegistry, RemoteRequest,
    RemoteResponse,
};
use crate::remote::cluster::builder::client::ClusterClientBuilder;
use crate::remote::cluster::builder::worker::ClusterWorkerBuilder;
use crate::remote::cluster::node::RemoteNode;
use crate::remote::handler::RemoteActorMessageMarker;
use crate::remote::net::client::RemoteClientStream;
use crate::remote::net::message::{ClientEvent, SessionEvent};
use crate::remote::net::proto::protocol::{ActorAddress, CreateActor};
use crate::remote::stream::mediator::StreamMediator;
use crate::remote::system::builder::RemoteActorSystemBuilder;
use crate::remote::{RemoteActorRef, RemoteMessageHeader};
use serde::Serialize;
use std::sync::Arc;
use tokio::sync::oneshot;

use crate::remote::net::StreamMessage;
use protobuf::Message as ProtoMessage;
use slog::Logger;
use uuid::Uuid;

pub mod builder;

#[derive(Clone)]
pub struct RemoteActorSystem {
    inner: Arc<RemoteSystemCore>,
}

#[derive(Clone)]
pub struct RemoteSystemCore {
    node_id: Uuid,
    inner: ActorSystem,
    handler_ref: LocalActorRef<RemoteHandler>,
    registry_ref: LocalActorRef<RemoteRegistry>,
    clients_ref: LocalActorRef<RemoteClientRegistry>,
    mediator_ref: Option<LocalActorRef<StreamMediator>>,
    types: Arc<RemoteHandlerTypes>,
}

impl RemoteActorSystem {
    pub fn builder() -> RemoteActorSystemBuilder {
        RemoteActorSystemBuilder::new()
    }

    pub fn cluster_worker(self) -> ClusterWorkerBuilder {
        ClusterWorkerBuilder::new(self)
    }

    pub fn cluster_client(self) -> ClusterClientBuilder {
        ClusterClientBuilder::new(self)
    }
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum RemoteActorErr {
    ActorUnavailable,
    ActorExists,
    RecipeSerializationErr,
    ActorNotSupported,
    NodeErr(NodeEventErr),
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum NodeEventErr {
    NodeUnreachable,
    Serialisation,
    ReceiveFailed,
}

impl RemoteActorSystem {
    pub fn node_tag(&self) -> &str {
        self.inner.types.node_tag()
    }

    pub async fn deploy_actor<F: ActorFactory>(
        &self,
        id: Option<ActorId>,
        recipe: F::Recipe,
        node: Option<Uuid>,
    ) -> Result<ActorRef<F::Actor>, RemoteActorErr> {
        let self_id = self.node_id();
        let id = id.map_or_else(new_actor_id, |id| id);
        let node = node.map_or_else(|| self_id, |n| n);
        let actor_type = F::Actor::type_name().into();

        let actor_recipe = recipe.write_to_bytes();
        if actor_recipe.is_none() {
            return Err(RemoteActorErr::RecipeSerializationErr);
        }

        let message_id = Uuid::new_v4();
        let message = CreateActor {
            actor_type,
            message_id: message_id.to_string(),
            actor_id: id.clone(),
            recipe: actor_recipe.unwrap(),
            ..CreateActor::default()
        };

        let mut actor_addr = None;
        if node == self_id {
            let local_create = self.handle_create_actor(message).await;
            if local_create.is_ok() {
                actor_addr = Some(ActorAddress::parse_from_bytes(&local_create.unwrap()).unwrap());
            } else {
                return Err(local_create.unwrap_err());
            }
        } else {
            match self
                .node_event::<ActorAddress>(message_id, SessionEvent::CreateActor(message), node)
                .await
            {
                Ok(address) => actor_addr = Some(address),
                Err(e) => return Err(RemoteActorErr::NodeErr(e)),
            }
        }

        match actor_addr {
            Some(actor_address) => {
                let actor_ref = RemoteActorRef::<F::Actor>::new(
                    actor_address.actor_id,
                    Uuid::parse_str(actor_address.node_id.as_str()).unwrap(),
                    self.clone(),
                );

                Ok(ActorRef::from(actor_ref))
            }
            None => Err(RemoteActorErr::ActorUnavailable),
        }
    }

    pub async fn node_event<T: ProtoMessage>(
        &self,
        message_id: Uuid,
        event: SessionEvent,
        node_id: Uuid,
    ) -> Result<T, NodeEventErr> {
        let (res_tx, res_rx) = oneshot::channel();

        trace!(
            &self.actor_system().log(),
            "message_id={}, created channel, storing request",
            &message_id
        );
        self.push_request(message_id, res_tx).await;

        trace!(
            &self.actor_system().log(),
            "message_id={}, emitting event to node_id={}",
            &message_id,
            &node_id
        );
        self.send_message(node_id, event).await;

        trace!(
            &self.actor_system().log(),
            "message_id={}, waiting for result",
            &message_id
        );
        match res_rx.await {
            Ok(RemoteResponse::Ok(res)) => match T::parse_from_bytes(&res) {
                Ok(res) => {
                    trace!(
                        &self.actor_system().log(),
                        "message_id={}, received result",
                        &message_id
                    );
                    Ok(res)
                }
                Err(_) => {
                    error!(
                        &self.actor_system().log(),
                        "message_id={}, failed to decode result from node_id={}",
                        &message_id,
                        &node_id
                    );
                    Err(NodeEventErr::Serialisation)
                }
            },
            _ => {
                error!(&self.actor_system().log(), "failed to receive result");
                Err(NodeEventErr::ReceiveFailed)
            }
        }
    }

    pub async fn handle_message(
        &self,
        identifier: String,
        actor_id: ActorId,
        buffer: &[u8],
    ) -> Result<Vec<u8>, RemoteActorErr> {
        let (tx, rx) = oneshot::channel();
        let handler = self.inner.types.message_handler(&identifier);

        if let Some(handler) = handler {
            handler.handle(actor_id, buffer, tx).await;
        };

        match rx.await {
            Ok(res) => Ok(res),
            Err(_e) => Err(RemoteActorErr::ActorUnavailable),
        }
    }

    pub async fn handle_create_actor(
        &self,
        mut args: CreateActor,
    ) -> Result<Vec<u8>, RemoteActorErr> {
        let (tx, rx) = oneshot::channel();

        let actor_id = if args.actor_id.is_empty() {
            None
        } else {
            Some(args.actor_id)
        };

        if let Some(actor_id) = &actor_id {
            if let Some(node_id) = self.locate_actor_node(actor_id.clone()).await {
                warn!(
                    &self.actor_system().log(),
                    "actor {} already exists on node: {}", &actor_id, &node_id
                );
                return Err(RemoteActorErr::ActorExists);
            }
        }

        let actor_id = actor_id.map_or_else(|| new_actor_id(), |id| id);
        args.actor_id = actor_id.clone();

        trace!(
            &self.actor_system().log(),
            "creating actor (actor_id={})",
            &actor_id
        );
        let handler = self.inner.types.actor_handler(&args.actor_type);

        if let Some(handler) = handler {
            handler.create(args, self.clone(), tx).await;
        } else {
            trace!(
                &self.actor_system().log(),
                "No handler found with the type: {}",
                &args.actor_type
            );
            return Err(RemoteActorErr::ActorNotSupported);
        }

        trace!(&self.actor_system().log(), "waiting for actor to start");

        match rx.await {
            Ok(res) => {
                trace!(
                    &self.actor_system().log(),
                    "actor started (actor_id={})",
                    &actor_id
                );

                Ok(res)
            }
            Err(_e) => Err(RemoteActorErr::ActorUnavailable),
        }
    }

    pub fn node_id(&self) -> Uuid {
        self.inner.node_id
    }

    pub fn handler_name<A: Actor, M: Message>(&self) -> Option<String> {
        let marker = RemoteActorMessageMarker::<A, M>::new();
        self.inner.types.handler_name(marker)
    }

    pub fn create_header<A: Actor, M: Message>(&self, id: &ActorId) -> Option<RemoteMessageHeader> {
        match self.handler_name::<A, M>() {
            Some(handler_type) => Some(RemoteMessageHeader {
                actor_id: id.clone(),
                handler_type,
            }),
            None => None,
        }
    }

    pub async fn register_client<T: RemoteClientStream>(&self, node_id: Uuid, client: T)
    where
        T: 'static + Sync + Send,
    {
        self.inner
            .clients_ref
            .send(RegisterClient(node_id, client))
            .await
            .unwrap()
    }

    pub async fn deregister_client(&self, node_id: Uuid) {
        self.inner
            .clients_ref
            .send(DeregisterClient(node_id))
            .await
            .unwrap()
    }

    pub async fn register_nodes(&self, nodes: Vec<RemoteNode>) {
        self.inner
            .registry_ref
            .send(RegisterNodes(nodes))
            .await
            .unwrap()
    }

    pub fn notify_register_nodes(&self, nodes: Vec<RemoteNode>) {
        self.inner
            .registry_ref
            .notify(RegisterNodes(nodes))
            .unwrap()
    }

    pub fn register_actor(&self, actor_id: ActorId, node_id: Option<Uuid>) {
        self.inner
            .registry_ref
            .notify(RegisterActor::new(actor_id, node_id));
    }

    pub async fn register_node(&self, node: RemoteNode) {
        self.inner
            .registry_ref
            .send(RegisterNode(node))
            .await
            .unwrap()
    }

    pub async fn get_nodes(&self) -> Vec<RemoteNode> {
        self.inner.registry_ref.send(GetNodes).await.unwrap()
    }

    pub async fn send_message(&self, node_id: Uuid, message: SessionEvent) {
        self.inner
            .clients_ref
            .send(ClientWrite(node_id, message))
            .await
            .unwrap()
    }

    pub async fn actor_ref<A: Actor>(&self, actor_id: ActorId) -> Option<ActorRef<A>> {
        let actor_type_name = A::type_name();
        let span = tracing::trace_span!(
            "RemoteActorSystem::actor_ref",
            actor_id = actor_id.as_str(),
            actor_type_name
        );
        let _enter = span.enter();

        match self.locate_actor_node(actor_id.clone()).await {
            Some(node_id) => {
                if node_id == self.inner.node_id {
                    self.inner
                        .inner
                        .get_tracked_actor(actor_id)
                        .await
                        .map(|actor_ref| ActorRef::from(actor_ref))
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

    pub async fn locate_actor_node(&self, actor_id: ActorId) -> Option<Uuid> {
        let span = tracing::trace_span!(
            "RemoteActorSystem::locate_actor_node",
            actor_id = actor_id.as_str()
        );
        let _enter = span.enter();

        trace!(
            &self.actor_system().log(),
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
                match tokio::time::timeout(tokio::time::Duration::from_secs(30), rx).await {
                    Ok(Ok(res)) => {
                        trace!(
                            &self.actor_system().log(),
                            "received actor node (current_node={}, actor_id={}, actor_node={:?})",
                            self.node_tag(),
                            &actor_id,
                            &res
                        );
                        res
                    }
                    Ok(Err(e)) => {
                        error!(&self.actor_system().log(), "error receiving result, {}", e);
                        None
                    }
                    Err(e) => {
                        error!(&self.actor_system().log(), "error receiving result, {}", e);
                        None
                    }
                }
            }
            Err(_e) => {
                error!(&self.actor_system().log(), "error sending message");
                None
            }
        }
    }

    pub async fn push_request(&self, id: Uuid, res_tx: oneshot::Sender<RemoteResponse>) {
        self.inner
            .handler_ref
            .send(PushRequest(id, RemoteRequest { res_tx }))
            .await
            .expect("push request send");
    }

    pub async fn pop_request(&self, id: Uuid) -> Option<oneshot::Sender<RemoteResponse>> {
        match self.inner.handler_ref.send(PopRequest(id)).await {
            Ok(s) => s.map(|s| s.res_tx),
            Err(_) => None,
        }
    }

    pub fn stream_mediator(&self) -> Option<&LocalActorRef<StreamMediator>> {
        self.inner.mediator_ref.as_ref()
    }

    pub fn actor_system(&self) -> &ActorSystem {
        &self.inner.actor_system()
    }
}

impl RemoteSystemCore {
    pub fn actor_system(&self) -> &ActorSystem {
        &self.inner
    }
}
