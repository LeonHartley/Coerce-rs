use crate::actor::message::{MessageUnwrapErr, MessageWrapErr};
use crate::actor::{ActorRefErr, ToActorId};
use crate::remote::net::proto::network::{
    ActorAddress, ClientErr, ClientHandshake, ClientResult, CreateActorEvent, Event,
    FindActorEvent, IdentifyEvent, MessageRequest, NodeIdentity, PingEvent, PongEvent, RaftRequest,
    SessionHandshake, StreamPublishEvent,
};
use crate::remote::net::{proto, StreamData};
use chrono::{DateTime, NaiveDateTime, Utc};
use protobuf::{Enum, Error, Message};
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;
use uuid::Uuid;

pub enum ClientEvent {
    Identity(NodeIdentity),
    Handshake(ClientHandshake),
    Result(ClientResult),
    Err(ClientErr),
    Ping(PingEvent),
    Pong(PongEvent),
}

#[derive(Debug)]
pub enum SessionEvent {
    Identify(IdentifyEvent),
    Ping(PingEvent),
    Pong(PongEvent),
    Handshake(SessionHandshake),
    NotifyActor(MessageRequest),
    CreateActor(CreateActorEvent),
    RegisterActor(ActorAddress),
    FindActor(FindActorEvent),
    StreamPublish(Arc<StreamPublishEvent>),
    Result(ClientResult),
    Err(ClientErr),
    Raft(RaftRequest),
}

impl SessionEvent {}

#[derive(Debug)]
pub struct ClientError {
    error: ActorRefErr,
    request_id: Uuid,
}

impl Display for ClientError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "error={}, request_id={}", &self.error, &self.request_id)
    }
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
                Some(Event::Ping) => Some(ClientEvent::Ping(
                    PingEvent::parse_from_bytes(message).unwrap(),
                )),
                Some(Event::Pong) => Some(ClientEvent::Pong(
                    PongEvent::parse_from_bytes(message).unwrap(),
                )),
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
                    IdentifyEvent::parse_from_bytes(message).unwrap(),
                )),
                Some(Event::Handshake) => Some(SessionEvent::Handshake(
                    SessionHandshake::parse_from_bytes(message).unwrap(),
                )),
                Some(Event::Ping) => Some(SessionEvent::Ping(
                    PingEvent::parse_from_bytes(message).unwrap(),
                )),
                Some(Event::Pong) => Some(SessionEvent::Pong(
                    PongEvent::parse_from_bytes(message).unwrap(),
                )),
                Some(Event::CreateActor) => Some(SessionEvent::CreateActor(
                    CreateActorEvent::parse_from_bytes(message).unwrap(),
                )),
                Some(Event::FindActor) => Some(SessionEvent::FindActor(
                    FindActorEvent::parse_from_bytes(message).unwrap(),
                )),
                Some(Event::NotifyActor) => Some(SessionEvent::NotifyActor(
                    MessageRequest::parse_from_bytes(message).unwrap(),
                )),
                Some(Event::RegisterActor) => Some(SessionEvent::RegisterActor(
                    ActorAddress::parse_from_bytes(message).unwrap(),
                )),
                Some(Event::StreamPublish) => Some(SessionEvent::StreamPublish(Arc::new(
                    StreamPublishEvent::parse_from_bytes(message).unwrap(),
                ))),
                Some(Event::Result) => Some(SessionEvent::Result(
                    ClientResult::parse_from_bytes(message).unwrap(),
                )),
                Some(Event::Err) => Some(SessionEvent::Err(
                    ClientErr::parse_from_bytes(message).unwrap(),
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
            SessionEvent::Err(e) => (Event::Err, e.write_to_bytes()),
            _ => return None,
        };

        write_event(event_id, message)
    }
}

fn write_event(event_id: Event, message: Result<Vec<u8>, Error>) -> Option<Vec<u8>> {
    match message {
        Ok(mut message) => {
            message.insert(0, event_id as u8);
            Some(message)
        }
        Err(_) => None,
    }
}

pub fn datetime_to_timestamp(
    date_time: &DateTime<Utc>,
) -> protobuf::well_known_types::timestamp::Timestamp {
    protobuf::well_known_types::timestamp::Timestamp {
        seconds: date_time.timestamp(),
        nanos: date_time.timestamp_subsec_nanos() as i32,
        ..Default::default()
    }
}

pub fn timestamp_to_datetime(
    timestamp: protobuf::well_known_types::timestamp::Timestamp,
) -> DateTime<Utc> {
    DateTime::<Utc>::from_utc(
        NaiveDateTime::from_timestamp_opt(timestamp.seconds, timestamp.nanos as u32).unwrap(),
        Utc,
    )
}

impl From<ActorRefErr> for proto::network::ActorRefErr {
    fn from(err: ActorRefErr) -> Self {
        use proto::network::actor_ref_err::ErrorType;
        use proto::network::MessageUnwrapErr as ProtoUnwrapErr;
        use proto::network::MessageWrapErr as ProtoWrapErr;

        let mut error = proto::network::ActorRefErr::default();
        error.type_ = match err {
            ActorRefErr::ActorUnavailable => ErrorType::ActorUnavailable,
            ActorRefErr::NotFound(actor_id) => {
                error.actor_id = actor_id.to_string();
                ErrorType::NotFound
            }
            ActorRefErr::AlreadyExists(actor_id) => {
                error.actor_id = actor_id.to_string();
                ErrorType::AlreadyExists
            }
            ActorRefErr::Serialisation(e) => {
                error.serialization_error = match e {
                    MessageWrapErr::NotTransmittable => ProtoWrapErr::WrapUnsupported,
                    MessageWrapErr::SerializationErr => ProtoWrapErr::SerializationErr,
                    MessageWrapErr::Unknown => ProtoWrapErr::UnknownWrapErr,
                }
                .into();

                ErrorType::Serialisation
            }
            ActorRefErr::Deserialisation(e) => {
                error.deserialization_error = match e {
                    MessageUnwrapErr::NotTransmittable => ProtoUnwrapErr::UnwrapUnsupported,
                    MessageUnwrapErr::DeserializationErr => ProtoUnwrapErr::DeserializationErr,
                    MessageUnwrapErr::Unknown => ProtoUnwrapErr::UnknownUnwrapErr,
                }
                .into();

                ErrorType::Deserialisation
            }
            ActorRefErr::Timeout { time_taken_millis } => {
                error.time_taken_millis = time_taken_millis;
                ErrorType::Timeout
            }
            ActorRefErr::ActorStartFailed => ErrorType::ActorStartFailed,
            ActorRefErr::InvalidRef => ErrorType::InvalidRef,
            ActorRefErr::ResultChannelClosed => ErrorType::ResultChannelClosed,
            ActorRefErr::ResultSendFailed => ErrorType::ResultSendFailed,
            ActorRefErr::NotSupported {
                actor_id,
                message_type,
                actor_type,
            } => {
                error.actor_id = actor_id.to_string();
                error.message_type = message_type;
                error.actor_type = actor_type;
                ErrorType::NotSupported
            }
            ActorRefErr::NotImplemented => ErrorType::NotImplemented,
        }
        .into();

        error
    }
}

impl From<proto::network::ActorRefErr> for ActorRefErr {
    fn from(err: proto::network::ActorRefErr) -> Self {
        use proto::network::actor_ref_err::ErrorType;
        use proto::network::MessageUnwrapErr as ProtoUnwrapErr;
        use proto::network::MessageWrapErr as ProtoWrapErr;

        match err.type_.unwrap() {
            ErrorType::ActorUnavailable => ActorRefErr::ActorUnavailable,
            ErrorType::NotFound => ActorRefErr::NotFound(err.actor_id.to_actor_id()),
            ErrorType::AlreadyExists => ActorRefErr::AlreadyExists(err.actor_id.to_actor_id()),
            ErrorType::Serialisation => {
                ActorRefErr::Serialisation(match err.serialization_error.unwrap() {
                    ProtoWrapErr::WrapUnsupported => MessageWrapErr::NotTransmittable,
                    ProtoWrapErr::SerializationErr => MessageWrapErr::SerializationErr,
                    ProtoWrapErr::UnknownWrapErr => MessageWrapErr::Unknown,
                })
            }
            ErrorType::Deserialisation => {
                ActorRefErr::Deserialisation(match err.deserialization_error.unwrap() {
                    ProtoUnwrapErr::UnwrapUnsupported => MessageUnwrapErr::NotTransmittable,
                    ProtoUnwrapErr::DeserializationErr => MessageUnwrapErr::DeserializationErr,
                    ProtoUnwrapErr::UnknownUnwrapErr => MessageUnwrapErr::Unknown,
                })
            }
            ErrorType::Timeout => ActorRefErr::Timeout {
                time_taken_millis: err.time_taken_millis,
            },
            ErrorType::ActorStartFailed => ActorRefErr::ActorStartFailed,
            ErrorType::InvalidRef => ActorRefErr::InvalidRef,
            ErrorType::ResultChannelClosed => ActorRefErr::ResultChannelClosed,
            ErrorType::ResultSendFailed => ActorRefErr::ResultSendFailed,
            ErrorType::NotSupported => ActorRefErr::NotSupported {
                actor_id: err.actor_id.to_actor_id(),
                message_type: err.message_type,
                actor_type: err.actor_type,
            },
            ErrorType::NotImplemented => ActorRefErr::NotImplemented,
        }
    }
}
