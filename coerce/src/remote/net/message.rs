use crate::actor::ActorId;
use crate::remote::cluster::node::RemoteNode;

use crate::remote::net::proto::protocol::{ActorAddress, ClientHandshake, CreateActor, MessageRequest, SessionHandshake, ClientErrorCode};
use uuid::Uuid;
use crate::remote::net::StreamMessage;

pub enum ClientEvent {
    Handshake(ClientHandshake),
    Result(Uuid, Vec<u8>),
    Err(Uuid, ClientErrorCode),
    Ping(Uuid),
    Pong(Uuid),
}

pub enum SessionEvent {
    Ping(Uuid),
    Pong(Uuid),
    Handshake(SessionHandshake),
    Message(MessageRequest),
    CreateActor(CreateActor),
    RegisterActor(ActorAddress),
    FindActor(Uuid, ActorId),
}

impl StreamMessage for ClientEvent {
    fn read_from_bytes(data: Vec<u8>) -> Option<Self> {
        unimplemented!()
    }

    fn write_to_bytes(&self) -> Option<Vec<u8>> {
        unimplemented!()
    }
}

impl StreamMessage for SessionEvent {
    fn read_from_bytes(data: Vec<u8>) -> Option<Self> {
        unimplemented!()
    }

    fn write_to_bytes(&self) -> Option<Vec<u8>> {
        unimplemented!()
    }
}