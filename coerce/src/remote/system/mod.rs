use crate::actor::message::Message;
use crate::actor::system::ActorSystem;
use crate::actor::{
    new_actor_id, Actor, ActorFactory, ActorId, ActorRecipe, ActorRef, ActorRefErr, BoxedActorRef,
    CoreActorRef, LocalActorRef,
};
use crate::remote::actor::message::{
    ClientWrite, DeregisterClient, GetActorNode, GetNodes, PopRequest, PushRequest, RegisterActor,
    RegisterClient, RegisterNode, RegisterNodes,
};
use crate::remote::actor::{
    RemoteClientRegistry, RemoteHandler, RemoteRegistry, RemoteRequest, RemoteResponse,
    RemoteSystemConfig,
};
use crate::remote::cluster::builder::client::ClusterClientBuilder;
use crate::remote::cluster::builder::worker::ClusterWorkerBuilder;
use crate::remote::cluster::node::RemoteNode;
use crate::remote::handler::{send_proto_result, RemoteActorMessageMarker};
use crate::remote::net::client::RemoteClientStream;
use crate::remote::net::message::{ClientEvent, SessionEvent};
use crate::remote::net::proto::protocol::{ActorAddress, ClientResult, CreateActor};
use crate::remote::stream::mediator::StreamMediator;
use crate::remote::system::builder::RemoteActorSystemBuilder;
use crate::remote::{RemoteActorRef, RemoteMessageHeader};
use serde::Serialize;
use std::sync::Arc;
use tokio::sync::oneshot;

use crate::actor::context::ActorContext;
use crate::actor::supervised::Supervised;
use crate::remote::heartbeat::Heartbeat;
use crate::remote::net::StreamData;
use crate::remote::raft::RaftSystem;
use protobuf::Message as ProtoMessage;
use serde::de::DeserializeOwned;
use std::error::Error;
use std::fmt::{Display, Formatter};
use uuid::Uuid;

pub mod builder;

#[derive(Clone)]
pub struct RemoteActorSystem {
    inner: Arc<RemoteSystemCore>,
}

pub type NodeId = u64;

#[derive(Clone)]
pub struct RemoteSystemCore {
    node_id: NodeId,
    inner: ActorSystem,
    handler_ref: LocalActorRef<RemoteHandler>,
    registry_ref: LocalActorRef<RemoteRegistry>,
    clients_ref: LocalActorRef<RemoteClientRegistry>,
    heartbeat_ref: Option<LocalActorRef<Heartbeat>>,
    mediator_ref: Option<LocalActorRef<StreamMediator>>,
    config: Arc<RemoteSystemConfig>,
    raft: Option<Arc<RaftSystem>>,
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

    pub fn config(&self) -> &RemoteSystemConfig {
        &self.inner.config
    }

    pub fn raft(&self) -> Option<&RaftSystem> {
        self.inner.raft.as_ref().map(|s| s.as_ref())
    }
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum RemoteActorErr {
    ActorUnavailable,
    ActorExists,
    RecipeSerializationErr,
    ActorNotSupported,
    NodeErr(NodeRpcErr),
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum NodeRpcErr {
    NodeUnreachable,
    Serialisation,
    ReceiveFailed,
}

impl Display for NodeRpcErr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl Error for NodeRpcErr {}

impl RemoteActorSystem {
    pub fn node_tag(&self) -> &str {
        self.inner.config.node_tag()
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
                .handle_create_actor(Some(id.clone()), actor_type, recipe, None)
                .await;
            if local_create.is_ok() {
                actor_addr = Some(ActorAddress::parse_from_bytes(&local_create.unwrap()).unwrap());
            } else {
                return Err(local_create.unwrap_err());
            }
        } else {
            let message = CreateActor {
                actor_type,
                message_id: message_id.to_string(),
                actor_id: id.clone(),
                recipe,
                ..CreateActor::default()
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
                    actor_address.actor_id,
                    actor_address.node_id,
                    self.clone(),
                );

                Ok(ActorRef::from(actor_ref))
            }
            None => Err(RemoteActorErr::ActorUnavailable),
        }
    }

    pub async fn node_rpc_proto<T: ProtoMessage>(
        &self,
        message_id: Uuid,
        event: SessionEvent,
        node_id: NodeId,
    ) -> Result<T, NodeRpcErr> {
        match self.node_rpc_raw(message_id, event, node_id).await {
            Ok(res) => match T::parse_from_bytes(&res) {
                Ok(res) => {
                    trace!(target: "NodeEvent", "message_id={}, received result", &message_id);
                    Ok(res)
                }
                Err(_) => {
                    error!(target: "NodeEvent", "message_id={}, failed to decode result from node_id={}", &message_id, &node_id);
                    Err(NodeRpcErr::Serialisation)
                }
            },
            _ => {
                error!(target: "NodeEvent", "failed to receive result");
                Err(NodeRpcErr::ReceiveFailed)
            }
        }
    }

    pub async fn node_rpc<T: StreamData>(
        &self,
        message_id: Uuid,
        event: SessionEvent,
        node_id: NodeId,
    ) -> Result<T, NodeRpcErr> {
        match self.node_rpc_raw(message_id, event, node_id).await {
            Ok(res) => match T::read_from_bytes(res) {
                Some(res) => {
                    trace!(target: "NodeRpc", "message_id={}, received result", &message_id);
                    Ok(res)
                }
                None => {
                    error!(target: "NodeRpc", "message_id={}, failed to decode result from node_id={}", &message_id, &node_id);
                    Err(NodeRpcErr::Serialisation)
                }
            },
            _ => {
                error!(target: "NodeRpc", "failed to receive result");
                Err(NodeRpcErr::ReceiveFailed)
            }
        }
    }

    pub async fn node_rpc_raw(
        &self,
        message_id: Uuid,
        event: SessionEvent,
        node_id: NodeId,
    ) -> Result<Vec<u8>, NodeRpcErr> {
        let (res_tx, res_rx) = oneshot::channel();

        trace!(target: "NodeRpc", "message_id={}, created channel, storing request", &message_id);
        self.push_request(message_id, res_tx).await;

        trace!(target: "NodeRpc", "message_id={}, emitting event to node_id={}", &message_id, &node_id);
        self.send_message(node_id, event).await;

        trace!(target: "NodeRpc", "message_id={}, waiting for result", &message_id);
        match res_rx.await {
            Ok(RemoteResponse::Ok(res)) => Ok(res),
            _ => {
                error!(target: "NodeEvent", "failed to receive result");
                Err(NodeRpcErr::ReceiveFailed)
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
        let handler = self.inner.config.message_handler(&identifier);

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
        actor_id: Option<ActorId>,
        actor_type: String,
        raw_recipe: Vec<u8>,
        supervisor_ctx: Option<&mut ActorContext>,
    ) -> Result<Vec<u8>, RemoteActorErr> {
        let (tx, rx) = oneshot::channel();

        if let Some(actor_id) = &actor_id {
            if let Some(node_id) = self.locate_actor_node(actor_id.clone()).await {
                warn!(target: "ActorDeploy", "actor {} already exists on node: {}", &actor_id, &node_id);
                return Err(RemoteActorErr::ActorExists);
            }
        }

        let actor_id = actor_id.map_or_else(|| new_actor_id(), |id| id);

        trace!(target: "ActorDeploy", "creating actor (actor_id={})", &actor_id);
        let handler = self.inner.config.actor_handler(&actor_type);

        if let Some(handler) = handler {
            let actor_ref = handler
                .create(Some(actor_id.clone()), raw_recipe, supervisor_ctx)
                .await;

            match actor_ref {
                Ok(actor_ref) => {
                    let result = ActorAddress {
                        actor_id: actor_ref.actor_id().clone(),
                        node_id: self.node_id(),
                        ..ActorAddress::default()
                    };

                    trace!(target: "RemoteHandler", "sending created actor ref");
                    send_proto_result(result, tx);
                }
                Err(_) => return Err(RemoteActorErr::ActorUnavailable),
            }
        } else {
            trace!(target: "ActorDeploy", "No handler found with the type: {}", &actor_type);
            return Err(RemoteActorErr::ActorNotSupported);
        }

        trace!(target: "ActorDeploy", "waiting for actor to start");

        match rx.await {
            Ok(res) => {
                trace!(target: "ActorDeploy", "actor started (actor_id={})", &actor_id);

                Ok(res)
            }
            Err(_e) => Err(RemoteActorErr::ActorUnavailable),
        }
    }

    pub fn node_id(&self) -> u64 {
        self.inner.node_id
    }

    pub fn handler_name<A: Actor, M: Message>(&self) -> Option<String> {
        let marker = RemoteActorMessageMarker::<A, M>::new();
        self.inner.config.handler_name(marker)
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

    pub async fn register_client<T: RemoteClientStream>(&self, node_id: NodeId, client: T)
    where
        T: 'static + Sync + Send,
    {
        self.inner
            .clients_ref
            .send(RegisterClient(node_id, client))
            .await
            .unwrap()
    }

    pub async fn deregister_client(&self, node_id: NodeId) {
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

    pub fn register_actor(&self, actor_id: ActorId, node_id: Option<NodeId>) {
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

    pub async fn send_message(&self, node_id: NodeId, message: SessionEvent) {
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

    pub async fn locate_actor_node(&self, actor_id: ActorId) -> Option<NodeId> {
        let span = tracing::trace_span!(
            "RemoteActorSystem::locate_actor_node",
            actor_id = actor_id.as_str()
        );
        let _enter = span.enter();

        trace!(target: "LocateActorNode", "locating actor node (current_node={}, actor_id={})", self.node_tag(), &actor_id);
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
                        trace!(target: "LocateActorNode", "received actor node (current_node={}, actor_id={}, actor_node={:?})", self.node_tag(), &actor_id, &res);
                        res
                    }
                    Ok(Err(e)) => {
                        error!(target: "LocateActorNode", "error receiving result, {}", e);
                        None
                    }
                    Err(e) => {
                        error!(target: "LocateActorNode", "error receiving result, {}", e);
                        None
                    }
                }
            }
            Err(_e) => {
                error!(target: "LocateActorNode", "error sending message");
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
