use bytes::{Buf, BufMut, BytesMut};

use byteorder::{ByteOrder, LittleEndian};
use std::io::Error;
use tokio_util::codec::{Decoder, Encoder};

pub struct NetworkCodec;

impl Encoder<Vec<u8>> for NetworkCodec {
    type Error = Error;

    fn encode(&mut self, item: Vec<u8>, dst: &mut BytesMut) -> Result<(), Error> {
        trace!(target: "NetworkCodec", "encoding msg");

        dst.reserve(4 + item.len());
        dst.put_i32_le(item.len() as i32);
        dst.put_slice(item.as_slice());

        Ok(())
    }
}

impl Decoder for NetworkCodec {
    type Item = Vec<u8>;
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Vec<u8>>, Error> {
        if src.is_empty() || src.remaining() <= 4 {
            return Ok(None);
        }

        trace!(target: "NetworkCodec", "decoding message");

        let len = LittleEndian::read_i32(src.as_ref()) as usize;
        if (src.remaining() - 4) < len {
            return Ok(None);
        }

        src.advance(4);
        let buf = src.split_to(len);
        Ok(Some(buf.to_vec()))
    }
}
