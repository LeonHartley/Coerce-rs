use crate::codec::MessageCodec;
use crate::context::RemoteActorContext;
use byteorder::{ByteOrder, LittleEndian};
use bytes::BytesMut;
use coerce_rt::actor::message::Message;
use serde::de::DeserializeOwned;
use std::io::Error;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::AsyncReadExt;

pub mod client;
pub mod codec;
pub mod server;

#[async_trait]
pub trait StreamReceiver<Msg: DeserializeOwned> {
    async fn on_recv(&mut self, msg: Msg, ctx: &mut RemoteActorContext);
}

pub struct StreamReceiverFuture<C: MessageCodec, M: DeserializeOwned>
where
    C: 'static + Sync + Send,
    M: 'static + Sync + Send,
{
    codec: C,
    stop_rx: tokio::sync::oneshot::Receiver<bool>,
    _m: PhantomData<M>,
}

impl<M: DeserializeOwned, C: MessageCodec> tokio::stream::Stream for StreamReceiverFuture<C, M>
where
    C: 'static + Sync + Send,
    M: 'static + Sync + Send,
{
    type Item = Option<M>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Option<M>>> {
        // stream closed
        Poll::Ready(None)
    }
}

pub async fn receive_loop<
    C: MessageCodec,
    M: DeserializeOwned,
    S: tokio::io::AsyncRead + Unpin,
    R: StreamReceiver<M>,
>(
    mut stream: S,
    mut context: RemoteActorContext,
    mut receiver: R,
    codec: C,
) where
    C: 'static + Sync + Send,
    S: 'static + Sync + Send,
    R: 'static + Sync + Send,
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
            error!(target: "RemoteReceive", "bytes_read = 0");
            break;
        } else if len == 0 {
            error!(target: "RemoteReceive", "len = 0");
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
            Some(msg) => receiver.on_recv(msg, &mut context).await,
            None => trace!(target: "RemoteReceive", "error decoding msg"),
        }
    }

    trace!(target: "RemoteReceive", "closed");
}
