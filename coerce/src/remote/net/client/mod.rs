use chrono::{DateTime, Utc};
use futures::channel::oneshot;
use std::collections::VecDeque;
use tokio::io::WriteHalf;
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use tokio_util::codec::FramedWrite;

use crate::actor::scheduler::timer::Timer;
use crate::actor::{Actor, IntoActor, LocalActorRef};
use crate::remote::net::codec::NetworkCodec;
use crate::remote::net::proto::network as proto;
use crate::remote::system::{NodeId, RemoteActorSystem};

pub mod connect;
pub mod ping;
pub mod receive;
pub mod send;

pub struct RemoteClient {
    addr: String,
    client_type: ClientType,
    remote_node_id: Option<NodeId>,
    state: Option<ClientState>,
    stop: Option<oneshot::Sender<bool>>,
    write_buffer_bytes_total: usize,
    write_buffer: VecDeque<Vec<u8>>,
    register_node_upon_connect: Option<bool>,
}

impl RemoteClient {
    pub async fn new(
        addr: String,
        remote_node_id: Option<NodeId>,
        system: RemoteActorSystem,
        client_type: ClientType,
        register_node_upon_connect: bool,
    ) -> Result<LocalActorRef<Self>, tokio::io::Error> {
        let actor_id = Some(format!("RemoteClient-{}", &addr));

        debug!(
            "Creating RemoteClient (actor_id={})",
            actor_id.as_ref().unwrap()
        );
        Ok(RemoteClient {
            addr,
            remote_node_id,
            client_type,
            stop: None,
            state: Some(ClientState::Idle {
                connection_attempts: 0,
            }),
            write_buffer: VecDeque::new(),
            write_buffer_bytes_total: 0,
            register_node_upon_connect: Some(register_node_upon_connect),
        }
        .into_anon_actor(actor_id, system.actor_system())
        .await
        .unwrap())
    }

    pub fn close(&mut self) -> bool {
        if let Some(stop) = self.stop.take() {
            stop.send(true).is_ok()
        } else {
            false
        }
    }
}

impl Actor for RemoteClient {}

pub enum ClientState {
    Idle {
        connection_attempts: usize,
    },
    Connected(ConnectionState),
    Quarantined {
        since: DateTime<Utc>,
        connection_attempts: usize,
    },
}

pub struct ConnectionState {
    node_id: NodeId,
    node_tag: String,
    node_started_at: DateTime<Utc>,
    write: FramedWrite<WriteHalf<TcpStream>, NetworkCodec>,
    receive_task: JoinHandle<()>,
    ping_timer: Option<Timer>,
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

impl ConnectionState {
    pub async fn disconnected(&mut self) {}
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
