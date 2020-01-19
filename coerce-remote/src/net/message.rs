use crate::cluster::node::RemoteNode;
use coerce_rt::actor::ActorId;
use std::net::SocketAddr;
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
    Result(Uuid, String),
    Err(Uuid, ClientError),
    Ping(Uuid),
    Pong(Uuid),
}

#[derive(Serialize, Deserialize)]
pub struct MessageRequest {
    pub id: Uuid,
    pub handler_type: String,
    pub actor: ActorId,
    pub message: String,
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
}
