use crate::codec::MessageCodec;

use crate::context::RemoteActorContext;

use serde::de::DeserializeOwned;
use std::future::Future;
use std::io::Error;

use std::pin::Pin;
use std::task::{Context, Poll};

use crate::net::codec::NetworkCodec;
use futures::StreamExt;
use tokio_util::codec::{Decoder, FramedRead};

pub mod client;
pub mod codec;
pub mod server;

#[async_trait]
pub trait StreamReceiver<Msg: DeserializeOwned> {
    async fn on_recv(&mut self, msg: Msg, ctx: &mut RemoteActorContext);
}

pub struct StreamReceiverFuture<S: tokio::io::AsyncRead> {
    stream: FramedRead<S, NetworkCodec>,
    stop_rx: tokio::sync::oneshot::Receiver<bool>,
}

impl<S: tokio::io::AsyncRead> StreamReceiverFuture<S> {
    pub fn new(
        stream: FramedRead<S, NetworkCodec>,
        stop_rx: tokio::sync::oneshot::Receiver<bool>,
    ) -> StreamReceiverFuture<S> {
        StreamReceiverFuture { stream, stop_rx }
    }
}

impl<S: tokio::io::AsyncRead> tokio::stream::Stream for StreamReceiverFuture<S>
where
    S: Unpin,
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
    R: StreamReceiver<M>,
    S: tokio::io::AsyncRead + Unpin,
>(
    mut context: RemoteActorContext,
    read: FramedRead<S, NetworkCodec>,
    stop_rx: tokio::sync::oneshot::Receiver<bool>,
    mut receiver: R,
    codec: C,
) where
    S: 'static + Sync + Send,
    C: 'static + Sync + Send,
    R: 'static + Sync + Send,
    M: 'static + Sync + Send,
{
    let mut fut = StreamReceiverFuture::new(read, stop_rx);

    while let Some(res) = fut.next().await {
        match res {
            Some(res) => match codec.decode_msg::<M>(res) {
                Some(msg) => receiver.on_recv(msg, &mut context).await,
                None => trace!(target: "RemoteReceive", "error decoding msg"),
            },
            None => trace!(target: "RemoteReceive", "error receiving msg"),
        }
    }

    trace!(target: "RemoteReceive", "closed");
}
