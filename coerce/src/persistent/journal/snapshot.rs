use crate::actor::message::{Envelope, EnvelopeType, Message, MessageUnwrapErr, MessageWrapErr};
use chrono::{DateTime, Utc};

pub struct JournalPayload {
    pub message_type: String,
    pub sequence: i64,
    pub bytes: Vec<u8>,
}

pub trait Snapshot: 'static + Sync + Send + Sized {
    fn into_envelope(self, envelope_type: EnvelopeType) -> Result<Envelope<Self>, MessageWrapErr> {
        match envelope_type {
            EnvelopeType::Local => Ok(Envelope::Local(self)),
            EnvelopeType::Remote => self.as_remote_envelope(),
        }
    }

    fn as_remote_envelope(&self) -> Result<Envelope<Self>, MessageWrapErr> {
        Err(MessageWrapErr::NotTransmittable)
    }

    fn from_envelope(envelope: Envelope<Self>) -> Result<Self, MessageUnwrapErr> {
        match envelope {
            Envelope::Local(msg) => Ok(msg),
            Envelope::Remote(bytes) => Self::from_remote_envelope(bytes),
        }
    }

    fn from_remote_envelope(_: Vec<u8>) -> Result<Self, MessageUnwrapErr> {
        Err(MessageUnwrapErr::NotTransmittable)
    }
}
