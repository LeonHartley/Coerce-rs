use crate::codec::MessageCodec;
use crate::context::RemoteActorContext;
use crate::net::{receive_loop, StreamReceiver};
use futures::SinkExt;
use tokio_util::codec::{Framed, FramedRead, FramedWrite};

use crate::net::client::RemoteClientErr;
use crate::net::codec::NetworkCodec;
use crate::net::message::{ClientEvent, SessionEvent};
use serde::Serialize;
use uuid::Uuid;

pub struct RemoteServer<C: MessageCodec> {
    codec: C,
    stop: Option<tokio::sync::oneshot::Sender<bool>>,
}

pub struct SessionMessageReceiver<C: MessageCodec> {
    write: FramedWrite<tokio::io::WriteHalf<tokio::net::TcpStream>, NetworkCodec>,
    codec: C,
}

#[derive(Debug)]
pub enum RemoteSessionErr {
    Encoding,
    StreamErr(tokio::io::Error),
}

impl<C: MessageCodec> SessionMessageReceiver<C>
where
    C: Sync + Send,
{
    pub fn new(
        write: FramedWrite<tokio::io::WriteHalf<tokio::net::TcpStream>, NetworkCodec>,
        codec: C,
    ) -> SessionMessageReceiver<C> {
        SessionMessageReceiver { write, codec }
    }

    pub async fn send<M: Serialize>(&mut self, message: M) -> Result<(), RemoteSessionErr>
    where
        M: Sync + Send,
    {
        match self.codec.encode_msg(message) {
            Some(message) => match self.write.send(message).await {
                Ok(()) => Ok(()),
                Err(e) => Err(RemoteSessionErr::StreamErr(e)),
            },
            None => Err(RemoteSessionErr::Encoding),
        }
    }
}

impl<C: MessageCodec> RemoteServer<C>
where
    C: 'static + Sync + Send,
{
    pub fn new(codec: C) -> Self {
        RemoteServer { codec, stop: None }
    }

    pub async fn start(
        &mut self,
        addr: String,
        context: RemoteActorContext,
    ) -> Result<(), tokio::io::Error> {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        let (stop_tx, _stop_rx) = tokio::sync::oneshot::channel();

        tokio::spawn(server_loop(listener, context, self.codec.clone()));
        self.stop = Some(stop_tx);
        Ok(())
    }

    pub fn stop(&mut self) -> bool {
        if let Some(stop) = self.stop.take() {
            stop.send(true).is_ok()
        } else {
            false
        }
    }
}

pub async fn server_loop<C: MessageCodec>(
    mut listener: tokio::net::TcpListener,
    context: RemoteActorContext,
    codec: C,
) where
    C: 'static + Sync + Send,
{
    loop {
        match listener.accept().await {
            Ok((socket, addr)) => {
                trace!(target: "RemoteServer", "client accepted {}", addr);
                let (read, write) = tokio::io::split(socket);

                let read = FramedRead::new(read, NetworkCodec);
                let write = FramedWrite::new(write, NetworkCodec);
                let (_stop_tx, stop_rx) = tokio::sync::oneshot::channel();

                tokio::spawn(receive_loop(
                    context.clone(),
                    read,
                    stop_rx,
                    SessionMessageReceiver::new(write, codec.clone()),
                    codec.clone(),
                ));
            }
            Err(e) => error!(target: "RemoteServer", "error accepting client: {:?}", e),
        }
    }
}

#[async_trait]
impl<C: MessageCodec> StreamReceiver<SessionEvent> for SessionMessageReceiver<C>
where
    C: Sync + Send,
{
    async fn on_recv(&mut self, msg: SessionEvent, ctx: &mut RemoteActorContext) {
        match msg {
            SessionEvent::Message {
                id,
                identifier,
                actor,
                message,
            } => {
                let result = ctx.handle(identifier, actor, message.as_bytes()).await;
            }
            SessionEvent::Ping(id) => {
                trace!(target: "RemoteServer", "ping received, sending pong");
                self.send(ClientEvent::Pong(id)).await;
            }
            SessionEvent::Pong(id) => {}
        }
    }
}
