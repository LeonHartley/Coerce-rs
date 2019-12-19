use crate::codec::MessageCodec;
use std::net::IpAddr;

pub mod tcp;

pub trait RemoteTransport<S, C>
where
    S: RemoteServer,
    C: RemoteClient,
{
    fn create_server<A>(&self, ip: A, port: u16) -> S
    where
        A: Into<IpAddr>;

    fn create_client<A>(&self, ip: A, port: u16) -> C
    where
        A: Into<IpAddr>;
}

#[async_trait]
pub trait RemoteServer {
    async fn on_connection();
}

#[async_trait]
pub trait RemoteClient {
    async fn on_message(data: Vec<u8>);
}
