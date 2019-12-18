use crate::codec::{MessageDecoder, MessageEncoder};
use serde::de::DeserializeOwned;
use serde::Serialize;

impl<T: Serialize> MessageEncoder for T
where
    T: Send + Sync,
{
    fn encode(&self) -> Option<Vec<u8>> {
        match serde_json::to_vec(self) {
            Ok(d) => Some(d),
            Err(_e) => None,
        }
    }
}

impl<T: DeserializeOwned> MessageDecoder for T
where
    T: Send + Sync,
{
    fn decode(buffer: Vec<u8>) -> Option<T> {
        match serde_json::from_slice(buffer.as_slice()) {
            Ok(d) => Some(d),
            Err(_e) => None,
        }
    }
}
