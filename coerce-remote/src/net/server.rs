use crate::codec::MessageCodec;
use crate::context::RemoteActorContext;
use crate::net::{receive_loop, StreamReceiver};
use tokio_util::codec::FramedRead;

use crate::net::codec::NetworkCodec;
use uuid::Uuid;

pub struct RemoteServer<C: MessageCodec> {
    codec: C,
    stop: Option<tokio::sync::oneshot::Sender<bool>>,
}

#[derive(Serialize, Deserialize)]
pub enum SessionEvent {
    Message {
        identifier: String,
        actor: Uuid,
        message: String,
    },
}

pub struct SessionMessageReceiver {
    write: tokio::io::WriteHalf<tokio::net::TcpStream>,
}

impl SessionMessageReceiver {
    pub fn new(write: tokio::io::WriteHalf<tokio::net::TcpStream>) -> SessionMessageReceiver {
        SessionMessageReceiver { write }
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

        let fut = tokio::spawn(server_loop(listener, context, self.codec.clone()));
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

                let framed = FramedRead::new(read, NetworkCodec);
                let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();

                tokio::spawn(receive_loop(
                    context.clone(),
                    framed,
                    stop_rx,
                    SessionMessageReceiver::new(write),
                    codec.clone(),
                ));
            }
            Err(e) => error!(target: "RemoteServer", "error accepting client: {:?}", e),
        }
    }
}

#[async_trait]
impl StreamReceiver<SessionEvent> for SessionMessageReceiver {
    async fn on_recv(&mut self, msg: SessionEvent, ctx: &mut RemoteActorContext) {
        match msg {
            SessionEvent::Message {
                identifier,
                actor,
                message,
            } => {
                let _result = ctx.handle(identifier, actor, message.as_bytes()).await;
            }
        }
    }
}
