use crate::codec::MessageCodec;
use crate::context::RemoteActorContext;
use byteorder::{ByteOrder, LittleEndian};
use bytes::BytesMut;
use serde::de::DeserializeOwned;
use tokio::io::AsyncReadExt;

pub mod client;
pub mod server;

#[async_trait]
pub trait StreamReceiver<Msg: DeserializeOwned> {
    async fn on_recv(&mut self, msg: Msg);
}

pub async fn receive_loop<C: MessageCodec, M: DeserializeOwned, R: StreamReceiver<M>>(
    mut stream: tokio::net::TcpStream,
    context: RemoteActorContext,
    mut receiver: R,
    codec: C,
) where
    M: 'static + Sync + Send,
{
    let mut len_buf = [0 as u8; 4];

    loop {
        let bytes_read = match stream.read(&mut len_buf).await {
            Ok(n) => n,
            Err(e) => {
                error!(target: "RemoteReceive", "failed to read length from stream, {:?}", e);
                return;
            }
        };

        let len = LittleEndian::read_i32(&mut len_buf) as usize;
        if bytes_read == 0 {
            return;
        } else if len == 0 {
            continue;
        }

        let mut buffer = BytesMut::with_capacity(len);
        let buff_read = match stream.read(&mut buffer).await {
            Ok(n) => n,
            Err(e) => {
                error!(target: "RemoteReceive", "failed to read message from stream, {:?}", e);
                return;
            }
        };

        trace!(target: "RemoteReceive", "received buffer with len {}", buff_read);

        match codec.decode_msg::<M>(buffer.to_vec()) {
            Some(msg) => receiver.on_recv(msg).await,
            None => trace!(target: "RemoteReceive", "error decoding msg"),
        }
    }
}
