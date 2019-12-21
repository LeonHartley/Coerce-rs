use crate::codec::MessageCodec;
use crate::context::RemoteActorContext;
use crate::net::codec::NetworkCodec;
use crate::net::{receive_loop, StreamReceiver};
use tokio_util::codec::FramedRead;

pub struct RemoteClient {
    write: tokio::io::WriteHalf<tokio::net::TcpStream>,
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

impl RemoteClient {
    pub async fn connect<C: MessageCodec>(
        addr: String,
        context: RemoteActorContext,
        codec: C,
    ) -> Result<RemoteClient, tokio::io::Error>
    where
        Self: 'static,
        C: 'static + Sync + Send,
    {
        let stream = tokio::net::TcpStream::connect(addr).await?;
        let (read, write) = tokio::io::split(stream);
        let framed = FramedRead::new(read, NetworkCodec);
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();

        tokio::spawn(receive_loop(
            context,
            framed,
            stop_rx,
            ClientMessageReceiver,
            codec,
        ));

        Ok(RemoteClient {
            write,
            stop: Some(stop_tx),
        })
    }

    pub fn close(&mut self) -> bool {
        if let Some(stop) = self.stop.take() {
            stop.send(true).is_ok()
        } else {
            false
        }
    }
}
