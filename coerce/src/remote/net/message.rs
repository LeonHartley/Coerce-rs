use crate::remote::net::proto::protocol::{
    ActorAddress, ClientErr, ClientHandshake, ClientResult, CreateActor, Event, FindActor,
    MessageRequest, Ping, Pong, SessionHandshake,
};
use crate::remote::net::StreamMessage;

use protobuf::{parse_from_bytes, Message, ProtobufEnum, ProtobufError};

pub enum ClientEvent {
    Handshake(ClientHandshake),
    Result(ClientResult),
    Err(ClientErr),
    Ping(Ping),
    Pong(Pong),
}

pub enum SessionEvent {
    Ping(Ping),
    Pong(Pong),
    Handshake(SessionHandshake),
    NotifyActor(MessageRequest),
    CreateActor(CreateActor),
    RegisterActor(ActorAddress),
    FindActor(FindActor),
}

impl StreamMessage for ClientEvent {
    fn read_from_bytes(data: Vec<u8>) -> Option<Self> {
        match data.split_first() {
            Some((event, message)) => match Event::from_i32(*event as i32) {
                Some(Event::Handshake) => {
                    Some(ClientEvent::Handshake(parse_from_bytes(message).unwrap()))
                }
                Some(Event::Result) => {
                    Some(ClientEvent::Result(parse_from_bytes(message).unwrap()))
                }
                Some(Event::Err) => Some(ClientEvent::Err(parse_from_bytes(message).unwrap())),
                Some(Event::Ping) => Some(ClientEvent::Ping(parse_from_bytes(message).unwrap())),
                Some(Event::Pong) => Some(ClientEvent::Pong(parse_from_bytes(message).unwrap())),
                _ => None,
            },
            None => None,
        }
    }

    fn write_to_bytes(&self) -> Option<Vec<u8>> {
        let (event_id, message) = match &self {
            ClientEvent::Handshake(e) => (Event::Handshake, e.write_to_bytes()),
            ClientEvent::Result(e) => (Event::Result, e.write_to_bytes()),
            ClientEvent::Err(e) => (Event::Err, e.write_to_bytes()),
            ClientEvent::Ping(e) => (Event::Ping, e.write_to_bytes()),
            ClientEvent::Pong(e) => (Event::Pong, e.write_to_bytes()),
        };

        write_event(event_id, message)
    }
}

impl StreamMessage for SessionEvent {
    fn read_from_bytes(data: Vec<u8>) -> Option<Self> {
        match data.split_first() {
            Some((event, message)) => match Event::from_i32(*event as i32) {
                Some(Event::Handshake) => {
                    Some(SessionEvent::Handshake(parse_from_bytes(message).unwrap()))
                }
                Some(Event::Ping) => Some(SessionEvent::Ping(parse_from_bytes(message).unwrap())),
                Some(Event::Pong) => Some(SessionEvent::Pong(parse_from_bytes(message).unwrap())),
                Some(Event::CreateActor) => Some(SessionEvent::CreateActor(
                    parse_from_bytes(message).unwrap(),
                )),
                Some(Event::FindActor) => {
                    Some(SessionEvent::FindActor(parse_from_bytes(message).unwrap()))
                }
                Some(Event::NotifyActor) => Some(SessionEvent::NotifyActor(
                    parse_from_bytes(message).unwrap(),
                )),
                Some(Event::RegisterActor) => Some(SessionEvent::RegisterActor(
                    parse_from_bytes(message).unwrap(),
                )),
                _ => None,
            },
            None => None,
        }
    }

    fn write_to_bytes(&self) -> Option<Vec<u8>> {
        let (event_id, message) = match &self {
            SessionEvent::Handshake(e) => (Event::Handshake, e.write_to_bytes()),
            SessionEvent::Ping(e) => (Event::Ping, e.write_to_bytes()),
            SessionEvent::Pong(e) => (Event::Pong, e.write_to_bytes()),
            SessionEvent::RegisterActor(e) => (Event::RegisterActor, e.write_to_bytes()),
            SessionEvent::NotifyActor(e) => (Event::NotifyActor, e.write_to_bytes()),
            SessionEvent::FindActor(e) => (Event::FindActor, e.write_to_bytes()),
            SessionEvent::CreateActor(e) => (Event::CreateActor, e.write_to_bytes()),
        };

        write_event(event_id, message)
    }
}

fn write_event(event_id: Event, message: Result<Vec<u8>, ProtobufError>) -> Option<Vec<u8>> {
    match message {
        Ok(mut message) => {
            let mut bytes = vec![];
            bytes.push(event_id as u8);
            bytes.append(&mut message);

            Some(bytes)
        }
        Err(_) => None,
    }
}
