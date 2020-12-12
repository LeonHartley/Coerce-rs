use crate::actor::message::{Envelope, Message, MessageUnwrapErr, MessageWrapErr};
use serde::de::DeserializeOwned;
use serde::Serialize;

pub trait RemoteMessage {
    type Result;
}

impl<M: RemoteMessage> Message for M
where
    Self: 'static + Sized,
    M: 'static + RemoteMessage + Sync + Send + Serialize + DeserializeOwned,
    <Self as RemoteMessage>::Result: 'static + Sync + Send + Serialize + DeserializeOwned,
{
    type Result = <Self as RemoteMessage>::Result;

    fn into_remote_envelope(self) -> Result<Envelope<Self>, MessageWrapErr> {
        serde_json::to_vec(&self)
            .map_err(|_e| MessageWrapErr::SerializationErr)
            .map(|bytes| Envelope::Remote(bytes))
    }

    fn from_remote_envelope(bytes: Vec<u8>) -> Result<Self, MessageUnwrapErr> {
        serde_json::from_slice(bytes.as_slice()).map_err(|_e| MessageUnwrapErr::DeserializationErr)
    }

    fn read_remote_result(bytes: Vec<u8>) -> Result<Self::Result, MessageUnwrapErr> {
        serde_json::from_slice(bytes.as_slice()).map_err(|_e| MessageUnwrapErr::DeserializationErr)
    }
}
