use crate::codec::MessageCodec;
use bytes::{BufMut, BytesMut};
use coerce_rt::actor::ActorId;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::io::Error;
use tokio_util::codec::{Decoder, Encoder};

pub struct NetworkCodec;

impl Encoder for NetworkCodec {
    type Item = Vec<u8>;
    type Error = Error;

    fn encode(&mut self, item: Vec<u8>, dst: &mut BytesMut) -> Result<(), Error> {
        dst.reserve(item.len());
        dst.put_slice(item.as_slice());

        Ok(())
    }
}

impl Decoder for NetworkCodec {
    type Item = Vec<u8>;
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Vec<u8>>, Error> {
        Ok(Some(src.to_vec()))
    }
}
