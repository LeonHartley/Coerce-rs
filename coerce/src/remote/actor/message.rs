use crate::remote::actor::RemoteRequest;
use crate::remote::cluster::node::RemoteNode;
use crate::remote::system::RemoteActorSystem;

use crate::actor::message::Message;
use crate::remote::net::client::RemoteClientStream;
use crate::remote::net::message::SessionEvent;

use crate::actor::ActorId;
use tokio::sync::oneshot::Sender;
use uuid::Uuid;

pub struct SetSystem(pub RemoteActorSystem);

impl Message for SetSystem {
    type Result = ();
}

pub struct GetNodes;

impl Message for GetNodes {
    type Result = Vec<RemoteNode>;
}

pub struct PushRequest(pub Uuid, pub RemoteRequest);

impl Message for PushRequest {
    type Result = ();
}

pub struct PopRequest(pub Uuid);

impl Message for PopRequest {
    type Result = Option<RemoteRequest>;
}

pub struct RegisterClient<T: RemoteClientStream>(pub Uuid, pub T);

impl<T: RemoteClientStream> Message for RegisterClient<T>
where
    T: 'static + Sync + Send,
{
    type Result = ();
}

pub struct RegisterNodes(pub Vec<RemoteNode>);

impl Message for RegisterNodes {
    type Result = ();
}

pub struct RegisterNode(pub RemoteNode);

impl Message for RegisterNode {
    type Result = ();
}

pub struct ClientWrite(pub Uuid, pub SessionEvent);

impl Message for ClientWrite {
    type Result = ();
}

#[derive(Debug)]
pub struct RegisterActor {
    pub actor_id: ActorId,
    pub node_id: Option<Uuid>,
}

impl RegisterActor {
    pub fn new(actor_id: ActorId, node_id: Option<Uuid>) -> RegisterActor {
        RegisterActor { actor_id, node_id }
    }
}

impl Message for RegisterActor {
    type Result = ();
}

pub struct GetActorNode {
    pub actor_id: ActorId,
    pub sender: tokio::sync::oneshot::Sender<Option<Uuid>>,
}

impl Message for GetActorNode {
    type Result = ();
}
