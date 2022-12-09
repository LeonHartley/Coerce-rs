use crate::actor::stream::ChatMessage;
use crate::protocol;
use coerce::actor::message::{Message, MessageUnwrapErr, MessageWrapErr};
use protobuf::well_known_types::wrappers::StringValue;
use protobuf::Message as ProtoMessage;
use protobuf::MessageField;

pub enum ClientEvent {
    Join(JoinChat),
    Chat(SendChatMessage),
    Leave(LeaveChat),
    Close,
}

impl Message for ClientEvent {
    type Result = ();
}

#[derive(Clone)]
pub struct JoinChat {
    pub chat_stream_id: String,
    pub join_token: Option<String>,
}

impl Message for JoinChat {
    type Result = ();

    fn as_bytes(&self) -> Result<Vec<u8>, MessageWrapErr> {
        protocol::chat::JoinChat {
            chat_stream_id: self.chat_stream_id.clone(),
            join_token: self
                .join_token
                .clone()
                .map(|n| StringValue {
                    value: n,
                    ..Default::default()
                })
                .into(),
            ..Default::default()
        }
        .write_to_bytes()
        .map_err(|_| MessageWrapErr::SerializationErr)
    }

    fn from_bytes(b: Vec<u8>) -> Result<Self, MessageUnwrapErr> {
        let msg = protocol::chat::JoinChat::parse_from_bytes(&b);
        match msg {
            Ok(msg) => Ok(Self {
                chat_stream_id: msg.chat_stream_id,
                join_token: msg.join_token.into_option().map(|s| s.value),
            }),
            Err(_) => Err(MessageUnwrapErr::DeserializationErr),
        }
    }
}

#[derive(Clone)]
pub struct SendChatMessage(pub ChatMessage);

impl From<ChatMessage> for protocol::chat::ChatMessage {
    fn from(val: ChatMessage) -> Self {
        protocol::chat::ChatMessage {
            chat_stream_id: val.chat_stream_id,
            message_id: val.message_id.map(|id| id.into()).into(),
            sender: val
                .sender
                .map(|sender| StringValue {
                    value: sender,
                    ..Default::default()
                })
                .into(),
            message: val.message,
            ..Default::default()
        }
    }
}

impl From<protocol::chat::ChatMessage> for ChatMessage {
    fn from(msg: protocol::chat::ChatMessage) -> Self {
        Self {
            chat_stream_id: msg.chat_stream_id,
            message_id: msg.message_id.into_option().map(|n| n.value),
            sender: msg.sender.into_option().map(|s| s.value),
            message: msg.message,
        }
    }
}

impl Message for SendChatMessage {
    type Result = ();

    fn as_bytes(&self) -> Result<Vec<u8>, MessageWrapErr> {
        protocol::chat::SendChatMessage {
            message: MessageField::from(Some(self.0.clone().into())),
            ..Default::default()
        }
        .write_to_bytes()
        .map_err(|_| MessageWrapErr::SerializationErr)
    }

    fn from_bytes(b: Vec<u8>) -> Result<Self, MessageUnwrapErr> {
        let msg = protocol::chat::SendChatMessage::parse_from_bytes(&b);
        match msg {
            Ok(msg) => match msg.message.into_option() {
                Some(msg) => Ok(Self(msg.into())),
                None => Err(MessageUnwrapErr::DeserializationErr),
            },
            Err(_) => Err(MessageUnwrapErr::DeserializationErr),
        }
    }
}
#[derive(Clone)]
pub struct LeaveChat(pub String);

impl Message for LeaveChat {
    type Result = ();

    fn as_bytes(&self) -> Result<Vec<u8>, MessageWrapErr> {
        protocol::chat::LeaveChat {
            chat_stream_id: self.0.clone(),
            ..Default::default()
        }
        .write_to_bytes()
        .map_err(|_| MessageWrapErr::SerializationErr)
    }

    fn from_bytes(b: Vec<u8>) -> Result<Self, MessageUnwrapErr> {
        let msg = protocol::chat::LeaveChat::parse_from_bytes(&b);
        match msg {
            Ok(msg) => Ok(Self(msg.chat_stream_id)),
            Err(_) => Err(MessageUnwrapErr::DeserializationErr),
        }
    }
}

pub fn parse_inbound_message(data: Vec<u8>) -> Option<ClientEvent> {
    match data.split_first() {
        Some((message_type, message)) => match *message_type {
            0 => Some(ClientEvent::Join(
                JoinChat::from_bytes(message.to_vec()).unwrap(),
            )),
            1 => Some(ClientEvent::Chat(
                SendChatMessage::from_bytes(message.to_vec()).unwrap(),
            )),
            2 => Some(ClientEvent::Leave(
                LeaveChat::from_bytes(message.to_vec()).unwrap(),
            )),
            _ => Some(ClientEvent::Close),
        },
        _ => Some(ClientEvent::Close),
    }
}

pub fn write_outbound_message<M: Message>(id: Option<u8>, event: M) -> Option<Vec<u8>> {
    event.as_bytes().map_or(None, |mut bytes| {
        if let Some(id) = id {
            bytes.insert(0, id);
        };

        Some(bytes)
    })
}
