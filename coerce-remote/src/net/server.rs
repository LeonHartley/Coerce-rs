use crate::codec::MessageCodec;
use crate::context::RemoteActorContext;
use crate::net::receive_loop;
use std::net::SocketAddr;
use std::str::FromStr;

pub struct RemoteServer<C: MessageCodec> {
    codec: C,
}

impl<C: MessageCodec> RemoteServer<C> {
    pub fn new(codec: C) -> Self {
        RemoteServer { codec }
    }

    pub async fn start(
        &self,
        addr: String,
        context: RemoteActorContext,
    ) -> Result<(), tokio::io::Error> {
        let mut listener = tokio::net::TcpListener::bind(addr).await?;

        tokio::spawn(server_loop(listener, context));
        Ok(())
    }
}

pub async fn server_loop(mut listener: tokio::net::TcpListener, context: RemoteActorContext) {
    loop {
        match listener.accept().await {
            Ok((socket, addr)) => {
                trace!(target: "RemoteServer", "client accepted {}", addr);
                tokio::spawn(receive_loop(socket, context.clone()));
            }
            Err(e) => error!(target: "RemoteServer", "error accepting client: {:?}", e),
        }
    }
}
