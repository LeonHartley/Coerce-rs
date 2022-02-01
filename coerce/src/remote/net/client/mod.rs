use crate::actor::context::{ActorContext, ActorStatus};
use crate::actor::message::{Handler, Message};
use crate::actor::system::ActorSystem;
use crate::actor::{Actor, ActorId, BoxedActorRef, IntoActor, LocalActorRef};
use crate::remote::net::client::connect::Connect;
use crate::remote::net::codec::NetworkCodec;
use crate::remote::net::message::SessionEvent;
use crate::remote::net::proto::network as proto;
use crate::remote::net::StreamData;
use crate::remote::system::{NodeId, RemoteActorSystem};
use chrono::{DateTime, Utc};
use futures::SinkExt;
use std::collections::VecDeque;
use std::intrinsics::unreachable;
use tokio::io::{AsyncWrite, WriteHalf};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_util::codec::FramedWrite;

pub mod connect;
pub mod receive;

pub struct RemoteClient {
    addr: String,
    client_type: ClientType,
    remote_node_id: Option<NodeId>,
    state: ClientState,
    stop: Option<oneshot::Sender<bool>>,
    write_buffer: VecDeque<Vec<u8>>,
}

impl RemoteClient {
    pub async fn new(
        addr: String,
        remote_node_id: Option<NodeId>,
        system: RemoteActorSystem,
        client_type: ClientType,
    ) -> Result<LocalActorRef<Self>, tokio::io::Error> {
        let actor_id = Some(format!("RemoteClient-{}", &addr));
        Ok(RemoteClient {
            addr,
            remote_node_id,
            client_type,
            stop: None,
            state: ClientState::Idle,
            write_buffer: VecDeque::new(),
        }
        .into_actor(actor_id, system.actor_system())
        .await
        .unwrap())
    }
}

impl Actor for RemoteClient {}

pub enum ClientState {
    Idle,
    Connected(ConnectionState),
    Quarantined { since: DateTime<Utc> },
}

pub struct ConnectionState {
    node_id: NodeId,
    node_tag: String,
    node_started_at: DateTime<Utc>,
    write: FramedWrite<WriteHalf<TcpStream>, NetworkCodec>,
    receive_task: JoinHandle<()>,
}

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

pub struct Write<M: StreamData>(pub M);

impl<M: StreamData> Message for Write<M> {
    type Result = Result<(), RemoteClientErr>;
}

#[async_trait]
impl<M: StreamData> Handler<Write<M>> for RemoteClient {
    async fn handle(
        &mut self,
        message: Write<M>,
        _ctx: &mut ActorContext,
    ) -> Result<(), RemoteClientErr> {
        self.write(message.0).await
    }
}

impl RemoteClient {
    pub async fn flush_buffered_writes(&mut self) {
        let mut connection_state = match &mut self.state {
            ClientState::Connected(connection_state) => connection_state
            _ => unreachable!(),
        };

        while let Some(buffered_message) = self.write_buffer.pop_front() {
            write_bytes(buffered_message, &mut connection_state.write).await;
        }
    }

    pub async fn write<M: StreamData>(&mut self, message: M) -> Result<(), RemoteClientErr>
    where
        M: Sync + Send,
    {
        if let Some(bytes) = message.write_to_bytes() {
            match &mut self.state {
                ClientState::Idle | ClientState::Quarantined { .. } => {
                    self.write_buffer.push_back(bytes);

                    debug!("attempt to write to addr={} but no connection is established, buffering message (total_buffered={})",
                        &self.addr,
                        self.write_buffer.len()
                    );

                    Ok(())
                }

                ClientState::Connected(state) => write_bytes(bytes, &mut state.write).await,
            }
        } else {
            Err(RemoteClientErr::Encoding)
        }
    }

    pub fn close(&mut self) -> bool {
        if let Some(stop) = self.stop.take() {
            stop.send(true).is_ok()
        } else {
            false
        }
    }
}

pub(crate) async fn write_bytes(
    bytes: Vec<u8>,
    writer: &mut FramedWrite<WriteHalf<TcpStream>, NetworkCodec>,
) -> Result<(), RemoteClientErr> {
    match writer.send(bytes).await {
        Ok(()) => Ok(()),
        Err(e) => Err(RemoteClientErr::StreamErr(e)),
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
