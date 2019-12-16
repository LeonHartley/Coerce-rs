pub mod json;

pub trait MessageDecoder: Sized + Send + Sync {
    fn decode(buffer: Vec<u8>) -> Option<Self>;
}

pub trait MessageEncoder: Sized + Send + Sync {
    fn encode(&self) -> Option<Vec<u8>>;
}
