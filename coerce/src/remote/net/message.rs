use crate::remote::cluster::node::RemoteNode;
use crate::remote::net::proto::network::{
    ActorAddress, ClientErr, ClientHandshake, ClientResult, CreateActor, Event, FindActor,
    Identify, MessageRequest, NodeIdentity, Ping, Pong, RaftRequest, SessionHandshake,
    StreamPublish,
};
use crate::remote::net::StreamData;
use chrono::{DateTime, NaiveDateTime, Utc};
use protobuf::{Message, ProtobufEnum, ProtobufError};

pub enum ClientEvent {
    Identity(NodeIdentity),
    Handshake(ClientHandshake),
    Result(ClientResult),
    Err(ClientErr),
    Ping(Ping),
    Pong(Pong),
}

#[derive(Debug)]
pub enum SessionEvent {
    Identify(Identify),
    Ping(Ping),
    Pong(Pong),
    Handshake(SessionHandshake),
    NotifyActor(MessageRequest),
    CreateActor(CreateActor),
    RegisterActor(ActorAddress),
    FindActor(FindActor),
    StreamPublish(StreamPublish),
    Result(ClientResult),
    Raft(RaftRequest),
}

impl StreamData for ClientEvent {
    fn read_from_bytes(data: Vec<u8>) -> Option<Self> {
        match data.split_first() {
            Some((event, message)) => match Event::from_i32(*event as i32) {
                Some(Event::Identity) => Some(ClientEvent::Identity(
                    NodeIdentity::parse_from_bytes(message).unwrap(),
                )),
                Some(Event::Handshake) => Some(ClientEvent::Handshake(
                    ClientHandshake::parse_from_bytes(message).unwrap(),
                )),
                Some(Event::Result) => Some(ClientEvent::Result(
                    ClientResult::parse_from_bytes(message).unwrap(),
                )),
                Some(Event::Err) => Some(ClientEvent::Err(
                    ClientErr::parse_from_bytes(message).unwrap(),
                )),
                Some(Event::Ping) => {
                    Some(ClientEvent::Ping(Ping::parse_from_bytes(message).unwrap()))
                }
                Some(Event::Pong) => {
                    Some(ClientEvent::Pong(Pong::parse_from_bytes(message).unwrap()))
                }
                _ => None,
            },
            None => None,
        }
    }

    fn write_to_bytes(&self) -> Option<Vec<u8>> {
        let (event_id, message) = match &self {
            ClientEvent::Identity(e) => (Event::Identity, e.write_to_bytes()),
            ClientEvent::Handshake(e) => (Event::Handshake, e.write_to_bytes()),
            ClientEvent::Result(e) => (Event::Result, e.write_to_bytes()),
            ClientEvent::Err(e) => (Event::Err, e.write_to_bytes()),
            ClientEvent::Ping(e) => (Event::Ping, e.write_to_bytes()),
            ClientEvent::Pong(e) => (Event::Pong, e.write_to_bytes()),
        };

        write_event(event_id, message)
    }
}

impl StreamData for SessionEvent {
    fn read_from_bytes(data: Vec<u8>) -> Option<Self> {
        match data.split_first() {
            Some((event, message)) => match Event::from_i32(*event as i32) {
                Some(Event::Identify) => Some(SessionEvent::Identify(
                    Identify::parse_from_bytes(message).unwrap(),
                )),
                Some(Event::Handshake) => Some(SessionEvent::Handshake(
                    SessionHandshake::parse_from_bytes(message).unwrap(),
                )),
                Some(Event::Ping) => {
                    Some(SessionEvent::Ping(Ping::parse_from_bytes(message).unwrap()))
                }
                Some(Event::Pong) => {
                    Some(SessionEvent::Pong(Pong::parse_from_bytes(message).unwrap()))
                }
                Some(Event::CreateActor) => Some(SessionEvent::CreateActor(
                    CreateActor::parse_from_bytes(message).unwrap(),
                )),
                Some(Event::FindActor) => Some(SessionEvent::FindActor(
                    FindActor::parse_from_bytes(message).unwrap(),
                )),
                Some(Event::NotifyActor) => Some(SessionEvent::NotifyActor(
                    MessageRequest::parse_from_bytes(message).unwrap(),
                )),
                Some(Event::RegisterActor) => Some(SessionEvent::RegisterActor(
                    ActorAddress::parse_from_bytes(message).unwrap(),
                )),
                Some(Event::StreamPublish) => Some(SessionEvent::StreamPublish(
                    StreamPublish::parse_from_bytes(message).unwrap(),
                )),
                Some(Event::Result) => Some(SessionEvent::Result(
                    ClientResult::parse_from_bytes(message).unwrap(),
                )),
                _ => None,
            },
            None => None,
        }
    }

    fn write_to_bytes(&self) -> Option<Vec<u8>> {
        let (event_id, message) = match self {
            SessionEvent::Handshake(e) => (Event::Handshake, e.write_to_bytes()),
            SessionEvent::Ping(e) => (Event::Ping, e.write_to_bytes()),
            SessionEvent::Pong(e) => (Event::Pong, e.write_to_bytes()),
            SessionEvent::RegisterActor(e) => (Event::RegisterActor, e.write_to_bytes()),
            SessionEvent::NotifyActor(e) => (Event::NotifyActor, e.write_to_bytes()),
            SessionEvent::FindActor(e) => (Event::FindActor, e.write_to_bytes()),
            SessionEvent::CreateActor(e) => (Event::CreateActor, e.write_to_bytes()),
            SessionEvent::StreamPublish(e) => (Event::StreamPublish, e.write_to_bytes()),
            SessionEvent::Result(e) => (Event::Result, e.write_to_bytes()),
            SessionEvent::Identify(e) => (Event::Identify, e.write_to_bytes()),
            _ => return None,
        };

        write_event(event_id, message)
    }
}

fn write_event(event_id: Event, message: Result<Vec<u8>, ProtobufError>) -> Option<Vec<u8>> {
    match message {
        Ok(mut message) => {
            message.insert(0, event_id as u8);
            Some(message)
        }
        Err(_) => None,
    }
}

pub fn datetime_to_timestamp(date_time: &DateTime<Utc>) -> protobuf::well_known_types::Timestamp {
    let mut t = protobuf::well_known_types::Timestamp::new();

    t.set_seconds(date_time.timestamp());
    t.set_nanos(date_time.timestamp_subsec_nanos() as i32);
    t
}

pub fn timestamp_to_datetime(timestamp: protobuf::well_known_types::Timestamp) -> DateTime<Utc> {
    DateTime::<Utc>::from_utc(
        NaiveDateTime::from_timestamp(timestamp.seconds, timestamp.nanos as u32),
        Utc,
    )
}
