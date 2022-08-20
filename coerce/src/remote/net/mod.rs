use crate::remote::system::RemoteActorSystem;

use std::future::Future;
use std::io::Error;

use std::pin::Pin;
use std::task::{Context, Poll};

use crate::remote::net::codec::NetworkCodec;
use futures::StreamExt;
use protobuf::Message;
use tokio_util::codec::FramedRead;

pub mod client;
pub mod codec;
pub mod message;
pub mod proto;
pub mod server;

pub trait StreamData: 'static + Send + Sync + Sized {
    fn read_from_bytes(data: Vec<u8>) -> Option<Self>;

    fn write_to_bytes(&self) -> Option<Vec<u8>>;
}

#[async_trait]
pub trait StreamReceiver {
    type Message: StreamData;

    async fn on_receive(&mut self, msg: Self::Message, sys: &RemoteActorSystem);

    async fn on_close(&mut self, sys: &RemoteActorSystem);

    async fn close(&mut self);

    fn should_close(&self) -> bool;
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
                error!(target: "RemoteStream", "{:?}", e);
                Some(None)
            }
            None => None,
        })
    }
}

pub async fn receive_loop<R: StreamReceiver, S: tokio::io::AsyncRead + Unpin>(
    addr: String,
    mut system: RemoteActorSystem,
    read: FramedRead<S, NetworkCodec>,
    mut receiver: R,
) where
    R: Send,
{
    let mut reader = read;
    while let Some(res) = reader.next().await {
        match res {
            Ok(res) => match R::Message::read_from_bytes(res) {
                Some(msg) => {
                    receiver.on_receive(msg, &system).await;
                    if receiver.should_close() {
                        break;
                    }
                }
                None => {
                    warn!(target: "RemoteReceive", "failed to decode message from addr={}", &addr)
                }
            },
            Err(e) => {
                warn!(target: "RemoteReceive", "stream connection lost (addr={}) - error: {}", &addr, e);
                break;
            }
        }
    }

    trace!(target: "RemoteReceive", "closed");
    receiver.on_close(&mut system).await;
}
