use crate::actor::ActorId;
use serde::{Deserialize, Serialize};

pub mod json;

pub trait MessageDecoder: Sized + Send + Sync {
    fn decode(buffer: Vec<u8>) -> Option<Self>;
}

pub trait MessageEncoder: Sized + Send + Sync {
    fn encode(&self) -> Option<Vec<u8>>;
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RemoteHandlerMessage {
    pub actor_id: ActorId,
    pub handler_type: String,
    pub message: String,
}
