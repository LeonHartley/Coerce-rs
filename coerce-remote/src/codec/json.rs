use crate::codec::MessageCodec;
use serde::de::DeserializeOwned;
use serde::Serialize;

pub struct JsonCodec {}

impl JsonCodec {
    pub fn new() -> JsonCodec {
        JsonCodec {}
    }
}

impl MessageCodec for JsonCodec {
    fn encode_msg<M: Serialize>(&self, message: M) -> Option<Vec<u8>>
    where
        M: Send + Sync,
    {
        message.encode()
    }

    fn decode_msg<M: DeserializeOwned>(&self, data: Vec<u8>) -> Option<M>
    where
        M: Send + Sync,
    {
        M::decode(data)
    }

    fn clone(&self) -> Self {
        JsonCodec {}
    }
}

trait MessageDecoder: Sized + Send + Sync {
    fn decode(buffer: Vec<u8>) -> Option<Self>;
}

trait MessageEncoder: Sized + Send + Sync {
    fn encode(&self) -> Option<Vec<u8>>;
}

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
