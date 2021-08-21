use crate::remote::system::RemoteActorSystem;

use std::future::Future;
use std::io::Error;

use std::pin::Pin;
use std::task::{Context, Poll};

use crate::remote::net::codec::NetworkCodec;
use futures::StreamExt;
use slog::Logger;
use tokio_util::codec::FramedRead;

pub mod client;
pub mod codec;
pub mod message;
pub mod proto;
pub mod server;

pub trait StreamMessage: 'static + Send + Sync + Sized {
    fn read_from_bytes(data: Vec<u8>) -> Option<Self>;

    fn write_to_bytes(&self) -> Option<Vec<u8>>;
}

#[async_trait]
pub trait StreamReceiver {
    type Message: StreamMessage;

    async fn on_receive(&mut self, msg: Self::Message, ctx: &RemoteActorSystem);

    async fn on_close(&mut self, ctx: &RemoteActorSystem);
}

pub struct StreamReceiverFuture<S: tokio::io::AsyncRead> {
    stream: FramedRead<S, NetworkCodec>,
    stop_rx: tokio::sync::oneshot::Receiver<bool>,
    log: Logger,
}

impl<S: tokio::io::AsyncRead> StreamReceiverFuture<S> {
    pub fn new(
        stream: FramedRead<S, NetworkCodec>,
        stop_rx: tokio::sync::oneshot::Receiver<bool>,
        log: Logger,
    ) -> StreamReceiverFuture<S> {
        StreamReceiverFuture {
            stream,
            stop_rx,
            log,
        }
    }
}

impl<S: tokio::io::AsyncRead> tokio_stream::Stream for StreamReceiverFuture<S>
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
                error!(&self.log, "error reading from stream - {:?}", e);
                Some(None)
            }
            None => None,
        })
    }
}

pub async fn receive_loop<R: StreamReceiver, S: tokio::io::AsyncRead + Unpin>(
    mut system: RemoteActorSystem,
    read: FramedRead<S, NetworkCodec>,
    stop_rx: tokio::sync::oneshot::Receiver<bool>,
    mut receiver: R,
    log: Logger,
) where
    R: Send,
{
    let mut fut = StreamReceiverFuture::new(read, stop_rx, log);
    while let Some(res) = fut.next().await {
        match res {
            Some(res) => match R::Message::read_from_bytes(res) {
                Some(msg) => receiver.on_receive(msg, &system).await,
                None => warn!(&fut.log, "error decoding msg"),
            },
            None => {
                error!(&fut.log, "error receiving msg");
                break;
            }
        }
    }

    trace!(&fut.log, "closed");
    receiver.on_close(&mut system).await;
}
