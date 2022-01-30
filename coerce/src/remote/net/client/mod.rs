use crate::actor::Actor;
use crate::remote::net::codec::NetworkCodec;
use crate::remote::net::message::SessionEvent;
use crate::remote::net::proto::network as proto;
use crate::remote::net::StreamData;
use crate::remote::system::NodeId;
use chrono::{DateTime, Utc};
use futures::SinkExt;
use tokio::io::WriteHalf;
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_util::codec::FramedWrite;

pub mod connect;
pub mod receive;

pub struct RemoteClient {
    pub addr: String,
    pub client_type: ClientType,
    pub state: ClientState,
    stop: Option<oneshot::Sender<bool>>,
}

impl RemoteClient {
    pub async fn new(
        addr: String,
        remote_node_id: Option<NodeId>,
        system: RemoteActorSystem,
        nodes: Option<Vec<RemoteNodeState>>,
        client_type: ClientType,
    ) -> Result<LocalActorRef<Self>, tokio::io::Error> {
        trace!("handshake ack (node_id={}, node_tag={})", node_id, node_tag);
        Ok(RemoteClient {
            write,
            node_id,
            node_tag,
            client_type,
            node_started_at,
            receive_task,
            stop: Some(stop_tx),
        }
            .into_actor(
                Some(format!("RemoteClient-{}", &node_id)),
                system.actor_system(),
            )
            .await
            .unwrap())
    }
}

pub enum ClientState {
    Connecting,
    Connected(ConnectionState),
    Quarantined,
}

pub struct ConnectionState {
    node_id: NodeId,
    node_tag: String,
    node_started_at: DateTime<Utc>,
    write: FramedWrite<WriteHalf<TcpStream>, NetworkCodec>,
    receive_task: JoinHandle<()>,
}

impl Actor for RemoteClient {}

pub struct HandshakeAcknowledge {
    node_id: NodeId,
    node_tag: String,
    node_started_at: DateTime<Utc>,
}

#[derive(Debug)]
pub enum RemoteClientErr {
    Encoding,
    StreamErr(tokio::io::Error),
}

#[derive(Copy, Clone, Debug)]
pub enum ClientType {
    Client,
    Worker,
}

#[async_trait]
pub trait RemoteClientStream {
    async fn send(&mut self, message: SessionEvent) -> Result<(), RemoteClientErr>;
}

impl RemoteClient {
    pub async fn write<M: StreamData>(&mut self, message: M) -> Result<(), RemoteClientErr>
    where
        M: Sync + Send,
    {
        write_msg(message, &mut self.write).await
    }

    pub fn close(&mut self) -> bool {
        if let Some(stop) = self.stop.take() {
            stop.send(true).is_ok()
        } else {
            false
        }
    }
}

#[async_trait]
impl RemoteClientStream for RemoteClient {
    async fn send(&mut self, message: SessionEvent) -> Result<(), RemoteClientErr> {
        self.write(message).await
    }
}

async fn write_msg<M: StreamData>(
    message: M,
    write: &mut FramedWrite<WriteHalf<TcpStream>, NetworkCodec>,
) -> Result<(), RemoteClientErr>
where
    M: Sync + Send,
{
    match message.write_to_bytes() {
        Some(message) => match write.send(message).await {
            Ok(()) => Ok(()),
            Err(e) => Err(RemoteClientErr::StreamErr(e)),
        },
        None => Err(RemoteClientErr::Encoding),
    }
}

impl From<proto::ClientType> for ClientType {
    fn from(client_type: proto::ClientType) -> Self {
        match client_type {
            proto::ClientType::Client => Self::Client,
            proto::ClientType::Worker => Self::Worker,
        }
    }
}

impl From<ClientType> for proto::ClientType {
    fn from(client_type: ClientType) -> Self {
        match client_type {
            ClientType::Client => Self::Client,
            ClientType::Worker => Self::Worker,
        }
    }
}
