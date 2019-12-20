use crate::codec::MessageCodec;
use crate::context::RemoteActorContext;
use crate::net::{receive_loop, StreamReceiver};

pub struct RemoteClient<C: MessageCodec> {
    codec: C,
}

#[derive(Serialize, Deserialize)]
pub enum ClientEvent {}

pub struct ClientMessageReceiver;

#[async_trait]
impl StreamReceiver<ClientEvent> for ClientMessageReceiver {
    async fn on_recv(&mut self, msg: ClientEvent) {
        unimplemented!()
    }
}

impl<C: MessageCodec> RemoteClient<C>
where
    C: 'static + Sync + Send,
{
    pub async fn connect(
        addr: String,
        context: RemoteActorContext,
        codec: C,
    ) -> Result<RemoteClient, tokio::io::Error> {
        let mut stream = tokio::net::TcpStream::connect(addr).await?;

        tokio::spawn(receive_loop(
            stream,
            context,
            ClientMessageReceiver,
            codec,
        ));

        Ok(())
    }
}
