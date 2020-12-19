use crate::remote::codec::MessageCodec;

use crate::remote::system::RemoteActorSystem;

use serde::de::DeserializeOwned;
use std::future::Future;
use std::io::Error;

use std::pin::Pin;
use std::task::{Context, Poll};

use crate::remote::net::codec::NetworkCodec;
use futures::StreamExt;
use tokio_util::codec::FramedRead;

pub mod client;
pub mod codec;
pub mod message;
pub mod proto;
pub mod server;

pub trait StreamMessage: Sized {
    fn read_from_bytes(data: Vec<u8>) -> Option<Self>;

    fn write_to_bytes(&self) -> Option<Vec<u8>>;
}

#[async_trait]
pub trait StreamReceiver {
    type Message: StreamMessage;

    async fn on_recv(&mut self, msg: Self::Message, ctx: &mut RemoteActorSystem);

    async fn on_close(&mut self, ctx: &mut RemoteActorSystem);
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
            Some(Err(e)) => {
                error!(target: "RemoteStream", "{:?}", e);
                Some(None)
            }
            None => None,
        })
    }
}

pub async fn receive_loop<
    R: StreamReceiver,
    S: tokio::io::AsyncRead + Unpin,
>(
    mut system: RemoteActorSystem,
    read: FramedRead<S, NetworkCodec>,
    stop_rx: tokio::sync::oneshot::Receiver<bool>,
    mut receiver: R,
) where
    R: Send,
{
    let mut fut = StreamReceiverFuture::new(read, stop_rx);
    while let Some(res) = fut.next().await {
        match res {
            Some(res) => match R::Message::read_from_bytes(res) {
                Some(msg) => receiver.on_recv(msg, &mut system).await,
                None => warn!(target: "RemoteReceive", "error decoding msg"),
            },
            None => {
                error!(target: "RemoteReceive", "error receiving msg");
                break;
            }
        }
    }

    trace!(target: "RemoteReceive", "closed");
    receiver.on_close(&mut system).await;
}
