use crate::remote::system::RemoteActorSystem;

use std::future::Future;
use std::io::Error;

use bytes::BytesMut;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::StreamExt;
use tokio_util::codec::{FramedRead, LengthDelimitedCodec};

pub mod client;
pub mod message;
pub mod metrics;
pub mod proto;
pub mod security;
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

    fn on_deserialisation_failed(&mut self);

    fn on_stream_lost(&mut self, error: Error);

    async fn close(&mut self);

    fn should_close(&self) -> bool;
}

pub struct StreamReceiverFuture<S: tokio::io::AsyncRead> {
    stream: FramedRead<S, LengthDelimitedCodec>,
    stop_rx: tokio::sync::oneshot::Receiver<bool>,
}

impl<S: tokio::io::AsyncRead> StreamReceiverFuture<S> {
    pub fn new(
        stream: FramedRead<S, LengthDelimitedCodec>,
        stop_rx: tokio::sync::oneshot::Receiver<bool>,
    ) -> StreamReceiverFuture<S> {
        StreamReceiverFuture { stream, stop_rx }
    }
}

impl<S: tokio::io::AsyncRead> tokio_stream::Stream for StreamReceiverFuture<S>
where
    S: Unpin,
{
    type Item = Option<BytesMut>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Option<BytesMut>>> {
        if let Poll::Ready(Ok(true)) = Pin::new(&mut self.stop_rx).poll(cx) {
            return Poll::Ready(None);
        }

        let result: Option<Result<BytesMut, Error>> =
            futures::ready!(Pin::new(&mut self.stream).poll_next(cx));

        Poll::Ready(match result {
            Some(Ok(message)) => Some(Some(message)),
            Some(Err(e)) => {
                error!("{:?}", e);
                Some(None)
            }
            None => None,
        })
    }
}

pub async fn receive_loop<R: StreamReceiver, S: tokio::io::AsyncRead + Unpin>(
    mut system: RemoteActorSystem,
    read: FramedRead<S, LengthDelimitedCodec>,
    mut receiver: R,
) where
    R: Send,
{
    let mut reader = read;
    while let Some(res) = reader.next().await {
        match res {
            Ok(res) => match R::Message::read_from_bytes(res.to_vec()) {
                Some(msg) => {
                    receiver.on_receive(msg, &system).await;
                    if receiver.should_close() {
                        break;
                    }
                }
                None => {
                    // TODO: either pass the buffer into here or more context, this is pretty useless at the moment..
                    receiver.on_deserialisation_failed();
                }
            },
            Err(e) => {
                receiver.on_stream_lost(e);
                break;
            }
        }
    }

    receiver.on_close(&mut system).await;
}
