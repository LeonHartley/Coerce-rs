use crate::actor::ActorId;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

pub mod json;

pub trait MessageCodec {
    fn encode_msg<M: Serialize>(&self, message: M) -> Option<Vec<u8>>
    where
        M: Send + Sync;

    fn decode_msg<M: DeserializeOwned>(&self, message: Vec<u8>) -> Option<M>
    where
        M: Send + Sync;

    fn clone(&self) -> Self;
}
