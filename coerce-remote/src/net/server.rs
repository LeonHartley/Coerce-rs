use crate::codec::MessageCodec;
use crate::context::RemoteActorContext;
use crate::net::{receive_loop, StreamReceiver};
use std::net::SocketAddr;
use std::str::FromStr;

pub struct RemoteServer<C: MessageCodec> {
    codec: C,
}

#[derive(Serialize, Deserialize)]
pub enum SessionEvent {}

pub struct SessionMessageReceiver;

impl<C: MessageCodec> RemoteServer<C>
where
    C: 'static + Sync + Send,
{
    pub fn new(codec: C) -> Self {
        RemoteServer { codec }
    }

    pub async fn start(
        &self,
        addr: String,
        context: RemoteActorContext,
    ) -> Result<(), tokio::io::Error> {
        let mut listener = tokio::net::TcpListener::bind(addr).await?;

        tokio::spawn(server_loop(listener, context, self.codec.clone()));
        Ok(())
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

                tokio::spawn(receive_loop(
                    socket,
                    context.clone(),
                    SessionMessageReceiver,
                    codec.clone(),
                ));
            }
            Err(e) => error!(target: "RemoteServer", "error accepting client: {:?}", e),
        }
    }
}

#[async_trait]
impl StreamReceiver<SessionEvent> for SessionMessageReceiver {
    async fn on_recv(&mut self, msg: SessionEvent) {
        unimplemented!()
    }
}
