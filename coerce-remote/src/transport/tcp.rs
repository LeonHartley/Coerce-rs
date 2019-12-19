use crate::codec::MessageCodec;
use crate::transport::{RemoteClient, RemoteServer, RemoteTransport};
use std::net::IpAddr;

pub struct TcpTransport<C: MessageCodec> {
    codec: C,
}

pub struct TcpServer<C: MessageCodec> {
    codec: C,
}

pub struct TcpClient<C: MessageCodec> {
    codec: C,
}

impl<C: MessageCodec> TcpClient<C> {
    pub fn new(codec: C) -> TcpClient<C> {
        TcpClient { codec }
    }
}

impl<C: MessageCodec> TcpServer<C> {
    pub fn new(codec: C) -> TcpServer<C> {
        TcpServer { codec }
    }
}

impl<C: MessageCodec> RemoteClient for TcpClient<C> {
    async fn on_message(data: Vec<u8>) {
        unimplemented!()
    }
}

impl<C: MessageCodec> RemoteServer for TcpServer<C> {
    async fn on_connection() {
        unimplemented!()
    }
}

impl<C: MessageCodec> TcpTransport<C> {
    pub fn new(codec: C) -> TcpTransport<C> {
        TcpTransport { codec }
    }
}

impl<C> RemoteTransport<TcpServer<C>, TcpClient<C>> for TcpTransport<C>
where
    C: MessageCodec,
{
    fn create_server<A>(&self, ip: A, port: u16) -> TcpServer<C>
    where
        A: Into<IpAddr>,
    {
        TcpServer::new(self.codec.clone())
    }

    fn create_client<A>(&self, ip: A, port: u16) -> TcpClient<C>
    where
        A: Into<IpAddr>,
    {
        TcpClient::new(self.codec.clone())
    }
}
