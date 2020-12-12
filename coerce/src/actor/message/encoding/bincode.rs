// use crate::actor::message::{Envelope, Message, MessageUnwrapErr, MessageWrapErr};
// use serde::de::DeserializeOwned;
// use serde::Serialize;
//
// pub trait BincodeMessage {
//     type Result;
// }
//
// impl<M: BincodeMessage> Message for M
// where
//     Self: 'static + Sized,
//     M: 'static + BincodeMessage + Sync + Send + Serialize + DeserializeOwned,
//     <Self as BincodeMessage>::Result: 'static + Sync + Send + Serialize + DeserializeOwned,
// {
//     type Result = <Self as BincodeMessage>::Result;
//
//     fn into_remote_envelope(self) -> Result<Envelope<Self>, MessageWrapErr> {
//         bincode::serialize(&self)
//             .map_err(|e| MessageWrapErr::SerializationErr)
//             .map(|bytes| Envelope::Remote(bytes))
//     }
//
//     fn from_remote_envelope(bytes: Vec<u8>) -> Result<Self, MessageUnwrapErr> {
//         bincode::deserialize(bytes.as_slice()).map_err(|e| MessageUnwrapErr::DeserializationErr)
//     }
//
//     fn read_remote_result(bytes: Vec<u8>) -> Result<Self::Result, MessageUnwrapErr> {
//         bincode::deserialize(bytes.as_slice()).map_err(|e| MessageUnwrapErr::DeserializationErr)
//     }
// }
