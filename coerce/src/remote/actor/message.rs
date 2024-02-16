use crate::remote::actor::RemoteRequest;
use crate::remote::cluster::node::{RemoteNode, RemoteNodeState};
use crate::remote::system::{NodeId, RemoteActorSystem};

use crate::actor::message::Message;
use crate::remote::net::client::{ClientType, RemoteClient};
use crate::remote::net::message::SessionEvent;

use crate::actor::{ActorId, LocalActorRef};

use uuid::Uuid;

pub struct SetRemote(pub RemoteActorSystem);

impl Message for SetRemote {
    type Result = ();
}

pub struct GetNodes;

impl Message for GetNodes {
    type Result = Vec<RemoteNodeState>;
}

pub struct PushRequest(pub Uuid, pub RemoteRequest);

impl Message for PushRequest {
    type Result = ();
}

pub struct PopRequest(pub Uuid);

impl Message for PopRequest {
    type Result = Option<RemoteRequest>;
}

pub struct NewClient {
    pub addr: String,
    pub client_type: ClientType,
    pub system: RemoteActorSystem,
}

impl Message for NewClient {
    type Result = Option<LocalActorRef<RemoteClient>>;
}

pub struct RemoveClient {
    pub addr: String,
    pub node_id: Option<NodeId>,
}

impl Message for RemoveClient {
    type Result = ();
}

pub struct ClientConnected {
    pub addr: String,
    pub remote_node_id: NodeId,
    pub client_actor_ref: LocalActorRef<RemoteClient>,
}

impl Message for ClientConnected {
    type Result = ();
}

pub struct RegisterNodes(pub Vec<RemoteNode>);

impl Message for RegisterNodes {
    type Result = ();
}

pub struct UpdateNodes(pub Vec<RemoteNodeState>);

impl Message for UpdateNodes {
    type Result = ();
}

pub struct RegisterNode(pub RemoteNode);

impl Message for RegisterNode {
    type Result = ();
}

pub struct NodeTerminated(pub NodeId);

impl Message for NodeTerminated {
    type Result = ();
}

pub struct ClientWrite(pub NodeId, pub SessionEvent);

impl Message for ClientWrite {
    type Result = ();
}

#[derive(Debug)]
pub struct RegisterActor {
    pub actor_id: ActorId,
    pub node_id: Option<NodeId>,
}

impl RegisterActor {
    pub fn new(actor_id: ActorId, node_id: Option<NodeId>) -> RegisterActor {
        RegisterActor { actor_id, node_id }
    }
}

impl Message for RegisterActor {
    type Result = ();
}

pub struct GetActorNode {
    pub actor_id: ActorId,
    pub sender: tokio::sync::oneshot::Sender<Option<NodeId>>,
}

impl Message for GetActorNode {
    type Result = ();
}
