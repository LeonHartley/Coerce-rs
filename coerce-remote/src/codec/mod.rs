use coerce_rt::actor::ActorId;
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

#[derive(Serialize, Deserialize, Debug)]
pub struct RemoteHandlerMessage<M: Serialize> {
    pub actor_id: ActorId,
    pub handler_type: String,
    pub message: M,
}
