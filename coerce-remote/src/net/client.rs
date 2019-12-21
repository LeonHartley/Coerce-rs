use crate::codec::MessageCodec;
use crate::context::RemoteActorContext;
use crate::net::codec::NetworkCodec;
use crate::net::{receive_loop, StreamReceiver};

use futures::SinkExt;
use serde::Serialize;
use tokio::io::AsyncReadExt;
use tokio_util::codec::{FramedRead, FramedWrite};

pub struct RemoteClient<C: MessageCodec> {
    codec: C,
    write: FramedWrite<tokio::io::WriteHalf<tokio::net::TcpStream>, NetworkCodec>,
    stop: Option<tokio::sync::oneshot::Sender<bool>>,
}

#[derive(Serialize, Deserialize)]
pub enum ClientEvent {}

pub struct ClientMessageReceiver;

#[async_trait]
impl StreamReceiver<ClientEvent> for ClientMessageReceiver {
    async fn on_recv(&mut self, _msg: ClientEvent, _ctx: &mut RemoteActorContext) {
        unimplemented!()
    }
}

#[derive(Debug)]
pub enum RemoteClientErr {
    Encoding,
    StreamErr(tokio::io::Error),
}

impl<C: MessageCodec> RemoteClient<C> {
    pub async fn connect(
        addr: String,
        context: RemoteActorContext,
        codec: C,
    ) -> Result<RemoteClient<C>, tokio::io::Error>
    where
        C: 'static + Sync + Send,
    {
        let stream = tokio::net::TcpStream::connect(addr).await?;
        let (read, write) = tokio::io::split(stream);

        let read = FramedRead::new(read, NetworkCodec);
        let write = FramedWrite::new(write, NetworkCodec);

        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();

        tokio::spawn(receive_loop(
            context,
            read,
            stop_rx,
            ClientMessageReceiver,
            codec.clone(),
        ));

        Ok(RemoteClient {
            write,
            codec,
            stop: Some(stop_tx),
        })
    }

    pub async fn write<M: Serialize>(&mut self, message: M) -> Result<(), RemoteClientErr>
    where
        M: Sync + Send,
    {
        match self.codec.encode_msg(message) {
            Some(message) => match self.write.send(message).await {
                Ok(()) => Ok(()),
                Err(e) => Err(RemoteClientErr::StreamErr(e)),
            },
            None => Err(RemoteClientErr::Encoding),
        }
    }

    pub fn close(&mut self) -> bool {
        if let Some(stop) = self.stop.take() {
            stop.send(true).is_ok()
        } else {
            false
        }
    }
}
