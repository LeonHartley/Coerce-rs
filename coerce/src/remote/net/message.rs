use crate::actor::ActorId;
use crate::remote::cluster::node::RemoteNode;

use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug)]
pub enum ClientError {
    ActorUnavailable,
    HandleError,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ClientHandshake {
    pub node_id: Uuid,
    pub nodes: Vec<RemoteNode>,
}

#[derive(Serialize, Deserialize)]
pub enum ClientEvent {
    Handshake(ClientHandshake),
    Result(Uuid, Vec<u8>),
    Err(Uuid, ClientError),
    Ping(Uuid),
    Pong(Uuid),
}

#[derive(Serialize, Deserialize)]
pub struct CreateActor {
    pub id: Uuid,
    pub actor_id: Option<ActorId>,
    pub actor_type: String,
    pub recipe: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
pub struct ActorCreated {
    pub id: ActorId,
    pub node_id: Uuid,
}

#[derive(Serialize, Deserialize)]
pub struct MessageRequest {
    pub id: Uuid,
    pub handler_type: String,
    pub actor: ActorId,
    pub message: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
pub struct SessionHandshake {
    pub node_id: Uuid,
    pub nodes: Vec<RemoteNode>,
    pub token: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
pub enum SessionEvent {
    Ping(Uuid),
    Pong(Uuid),
    Handshake(SessionHandshake),
    Message(MessageRequest),
    CreateActor(CreateActor),
}
