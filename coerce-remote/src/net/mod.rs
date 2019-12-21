use crate::codec::MessageCodec;
use crate::context::RemoteActorContext;
use byteorder::{ByteOrder, LittleEndian};
use bytes::BytesMut;
use coerce_rt::actor::message::Message;
use futures::Stream;
use futures::{AsyncReadExt, SinkExt};
use serde::de::DeserializeOwned;
use std::future::Future;
use std::io::Error;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::stream::StreamExt;
use tokio_util::codec::{Decoder, Encoder, FramedRead};

pub mod client;
pub mod codec;
pub mod server;

#[async_trait]
pub trait StreamReceiver<Msg: DeserializeOwned> {
    async fn on_recv(&mut self, msg: Msg, ctx: &mut RemoteActorContext);
}

pub struct StreamReceiverFuture<C>
where
    C: Decoder,
{
    stream: FramedRead<tokio::net::TcpStream, C>,
    stop_rx: tokio::sync::oneshot::Receiver<bool>,
}

impl<C: Decoder> tokio::stream::Stream for StreamReceiverFuture<C>
where
    C: Decoder<Item = Vec<u8>, Error = Error>,
{
    type Item = Option<Vec<u8>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Option<Vec<u8>>>> {
        if let Poll::Ready(Ok(true)) = Pin::new(&mut self.stop_rx).poll(cx) {
            return Poll::Ready(None);
        }

        let result: Option<Result<Vec<u8>, Error>> =
            futures::ready!(Pin::new(&mut self.stream).poll_next(cx));

        Poll::Ready(match result {
            Some(Ok(message)) => Some(Some(message)),
            Some(Err(_)) => Some(None),
            None => None,
        })
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
    //    loop {
    //
    //        match codec.decode_msg::<M>(buffer.to_vec()) {
    //            Some(msg) => receiver.on_recv(msg, &mut context).await,
    //            None => trace!(target: "RemoteReceive", "error decoding msg"),
    //        }
    //    }

    trace!(target: "RemoteReceive", "closed");
}
