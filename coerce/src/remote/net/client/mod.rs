use chrono::{DateTime, Utc};
use std::collections::VecDeque;
use std::time::{Duration, Instant};
use tokio::io::WriteHalf;
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::RecvError;
use tokio::sync::oneshot::{Receiver, Sender};
use tokio::task::JoinHandle;
use tokio::time::error::Elapsed;
use tokio_util::codec::FramedWrite;

use crate::actor::context::{ActorContext, ActorStatus};
use crate::actor::message::{Handler, Message};
use crate::actor::scheduler::timer::Timer;
use crate::actor::system::ActorSystem;
use crate::actor::{Actor, ActorId, ActorRefErr, BoxedActorRef, IntoActor, LocalActorRef};
use crate::remote::cluster::node::{NodeIdentity, RemoteNode, RemoteNodeState};
use crate::remote::net::client::connect::Connect;
use crate::remote::net::client::receive::HandshakeAcknowledge;
use crate::remote::net::codec::NetworkCodec;
use crate::remote::net::proto::network as proto;
use crate::remote::system::{NodeId, RemoteActorErr, RemoteActorSystem};

pub mod connect;
pub mod ping;
pub mod receive;
pub mod send;

pub struct RemoteClient {
    addr: String,
    client_type: ClientType,
    remote_node: Option<RemoteNode>,
    state: Option<ClientState>,
    stop: Option<oneshot::Sender<bool>>,
    write_buffer_bytes_total: usize,
    write_buffer: VecDeque<Vec<u8>>,
    on_identified_callbacks: Vec<Sender<Option<NodeIdentity>>>,
    on_handshake_ack_callbacks: Vec<Sender<()>>,
}

pub struct RemoteClientRef {
    client: LocalActorRef<RemoteClient>,
}

impl RemoteClient {
    pub async fn new(
        addr: String,
        system: RemoteActorSystem,
        client_type: ClientType,
    ) -> LocalActorRef<Self> {
        let actor_id = Some(format!("RemoteClient-{}", &addr));

        debug!(
            "Creating RemoteClient (actor_id={})",
            actor_id.as_ref().unwrap()
        );

        RemoteClient {
            addr,
            client_type,
            remote_node: None,
            stop: None,
            state: Some(ClientState::Idle {
                connection_attempts: 0,
            }),
            write_buffer: VecDeque::new(),
            write_buffer_bytes_total: 0,
            on_identified_callbacks: vec![],
            on_handshake_ack_callbacks: vec![],
        }
        .into_anon_actor(actor_id, system.actor_system())
        .await
        .unwrap()
    }

    pub fn close(&mut self) -> bool {
        if let Some(stop) = self.stop.take() {
            stop.send(true).is_ok()
        } else {
            false
        }
    }
}

pub struct Identify {
    callback: Sender<Option<NodeIdentity>>,
}

impl Message for Identify {
    type Result = ();
}

#[async_trait]
impl Handler<Identify> for RemoteClient {
    async fn handle(&mut self, message: Identify, ctx: &mut ActorContext) {
        match &self.state {
            Some(ClientState::Connected(state)) => {
                let _ = message.callback.send(Some(state.identity.clone()));
            }
            _ => {
                self.on_identified_callbacks.push(message.callback);
            }
        }
    }
}

impl RemoteClientRef {
    pub async fn identify(&self) -> Result<Option<NodeIdentity>, ActorRefErr> {
        const REMOTE_CLIENT_IDENTIFY_TIMEOUT: Duration =
            Duration::from_secs(3 /*TODO: pull this from config*/);

        let (tx, rx) = oneshot::channel();
        if let Err(e) = self.client.notify(Identify { callback: tx }) {
            Err(e)
        } else {
            await_timeout(REMOTE_CLIENT_IDENTIFY_TIMEOUT, rx).await
        }
    }

    pub async fn handshake(&self, seed_nodes: Vec<RemoteNode>) -> Result<(), ActorRefErr> {
        const REMOTE_CLIENT_HANDSHAKE_TIMEOUT: Duration =
            Duration::from_secs(3 /*TODO: pull this from config*/);

        let (tx, rx) = oneshot::channel();

        info!("notify beginhandshake");
        if let Err(e) = self.client.notify(BeginHandshake {
            seed_nodes,
            on_handshake_complete: tx,
        }) {
            info!("err beginhandshake");
            Err(e)
        } else {
            await_timeout(REMOTE_CLIENT_HANDSHAKE_TIMEOUT, rx).await
        }
    }
}

async fn await_timeout<T>(timeout: Duration, rx: Receiver<T>) -> Result<T, ActorRefErr> {
    let now = Instant::now();
    match tokio::time::timeout(timeout, rx).await {
        Ok(res) => match res {
            Ok(res) => Ok(res),
            Err(_) => Err(ActorRefErr::ResultChannelClosed),
        },
        Err(_) => Err(ActorRefErr::Timeout {
            time_taken_millis: now.elapsed().as_millis(),
        }),
    }
}

impl From<LocalActorRef<RemoteClient>> for RemoteClientRef {
    fn from(client: LocalActorRef<RemoteClient>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl Actor for RemoteClient {
    async fn started(&mut self, ctx: &mut ActorContext) {
        let _ = self.actor_ref(ctx).notify(Connect {});
    }
}

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
    identity: NodeIdentity,
    handshake: HandshakeStatus,
    write: FramedWrite<WriteHalf<TcpStream>, NetworkCodec>,
    receive_task: JoinHandle<()>,
    ping_timer: Option<Timer>,
}

pub enum HandshakeStatus {
    None,
    Pending,
    Acknowledged(HandshakeAcknowledge),
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

pub struct BeginHandshake {
    seed_nodes: Vec<RemoteNode>,
    on_handshake_complete: Sender<()>,
}

impl Message for BeginHandshake {
    type Result = ();
}

impl ConnectionState {
    pub async fn disconnected(&mut self) {}
}

impl ClientState {
    pub fn is_connected(&self) -> bool {
        match &self {
            ClientState::Connected(_) => true,
            _ => false,
        }
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
