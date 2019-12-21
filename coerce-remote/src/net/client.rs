use crate::codec::MessageCodec;
use crate::context::RemoteActorContext;
use crate::net::{receive_loop, StreamReceiver};
use std::net::Shutdown;
use std::time::Duration;
use tokio::io::AsyncWriteExt;

pub struct RemoteClient {
    write: tokio::io::WriteHalf<tokio::net::TcpStream>,
}

#[derive(Serialize, Deserialize)]
pub enum ClientEvent {}

pub struct ClientMessageReceiver;

#[async_trait]
impl StreamReceiver<ClientEvent> for ClientMessageReceiver {
    async fn on_recv(&mut self, msg: ClientEvent, ctx: &mut RemoteActorContext) {
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
        let (read, mut write) = tokio::io::split(stream);

        tokio::spawn(receive_loop(read, context, ClientMessageReceiver, codec));

        Ok(RemoteClient { write })
    }
}
