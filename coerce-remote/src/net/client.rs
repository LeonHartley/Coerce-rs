use crate::codec::MessageCodec;
use crate::context::RemoteActorContext;
use crate::net::receive_loop;

pub struct RemoteClient<C: MessageCodec> {
    codec: C,
}

impl<C: MessageCodec> RemoteClient<C> {
    pub fn new(codec: C) -> Self {
        RemoteClient { codec }
    }

    pub async fn connect(
        &self,
        addr: String,
        context: RemoteActorContext,
    ) -> Result<(), tokio::io::Error> {
        let mut stream = tokio::net::TcpStream::connect(addr).await?;

        tokio::spawn(receive_loop(stream, context));
        Ok(())
    }
}
