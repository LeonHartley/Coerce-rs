use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::SinkExt;
use std::collections::VecDeque;
use std::fmt::{Display, Formatter};
use std::time::{Duration, Instant};
use tokio::io::WriteHalf;
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::sync::oneshot::{Receiver, Sender};
use tokio::task::JoinHandle;
use tokio_util::codec::{FramedWrite, LengthDelimitedCodec};
use uuid::Uuid;

use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message};
use crate::actor::scheduler::timer::Timer;
use crate::actor::{Actor, ActorRefErr, IntoActor, LocalActorRef};

use crate::remote::cluster::node::{NodeIdentity, RemoteNode};
use crate::remote::net::client::connect::Connect;
use crate::remote::net::client::receive::HandshakeAcknowledge;
use crate::remote::net::client::send::write_bytes;
use crate::remote::net::message::SessionEvent;
use crate::remote::net::proto::network as proto;
use crate::remote::net::proto::network::PingEvent;
use crate::remote::net::StreamData;
use crate::remote::system::{NodeId, RemoteActorSystem};

pub mod connect;
pub mod ping;
pub mod receive;
pub mod send;

pub struct RemoteClient {
    addr: String,
    node_id: Option<NodeId>,
    client_type: ClientType,
    state: Option<ClientState>,
    stop: Option<Sender<bool>>,
    write_buffer_bytes_total: usize,
    write_buffer: VecDeque<Vec<u8>>,
    on_identified_callbacks: Vec<Sender<Option<NodeIdentity>>>,
    on_handshake_ack_callbacks: Vec<HandshakeAckCallback>,
    ping_timer: Option<Timer>,
}

struct HandshakeAckCallback {
    request_id: Uuid,
    callback: Sender<()>,
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
        let actor_id = Some(format!("remote-client-{}", &addr));
        debug!(
            "Creating RemoteClient (actor_id={})",
            actor_id.as_ref().unwrap()
        );

        RemoteClient {
            addr,
            client_type,
            node_id: None,
            stop: None,
            state: Some(ClientState::Idle {
                connection_attempts: 0,
            }),
            write_buffer: VecDeque::new(),
            write_buffer_bytes_total: 0,
            on_identified_callbacks: vec![],
            on_handshake_ack_callbacks: vec![],
            ping_timer: None,
        }
        .into_actor(actor_id, system.actor_system())
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
    async fn handle(&mut self, message: Identify, _ctx: &mut ActorContext) {
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

const REMOTE_CLIENT_HANDSHAKE_TIMEOUT: Duration =
    Duration::from_secs(3 /*TODO: pull this from config*/);
const REMOTE_CLIENT_HANDSHAKE_MAX_ATTEMPTS: usize = 5;

impl RemoteClientRef {
    pub async fn identify(&self) -> Result<Option<NodeIdentity>, ActorRefErr> {
        const REMOTE_CLIENT_IDENTIFY_TIMEOUT: Duration =
            Duration::from_secs(1 /*TODO: pull this from config*/);

        let (tx, rx) = oneshot::channel();
        if let Err(e) = self.client.notify(Identify { callback: tx }) {
            Err(e)
        } else {
            await_timeout(REMOTE_CLIENT_IDENTIFY_TIMEOUT, rx).await
        }
    }

    pub async fn handshake(
        &self,
        request_id: Uuid,
        seed_nodes: Vec<RemoteNode>,
    ) -> Result<(), ActorRefErr> {
        let start = Instant::now();
        for _attempt in 0..REMOTE_CLIENT_HANDSHAKE_MAX_ATTEMPTS {
            match self.handshake_attempt(request_id, seed_nodes.clone()).await {
                Err(ActorRefErr::Timeout { .. }) => {
                    warn!(
                        "handshake request to node (addr={}) timed out",
                        &self.client.actor_id(),
                    );
                }

                result => return result,
            }
        }

        Err(ActorRefErr::Timeout {
            time_taken_millis: start.elapsed().as_millis() as u64,
        })
    }

    async fn handshake_attempt(
        &self,
        request_id: Uuid,
        seed_nodes: Vec<RemoteNode>,
    ) -> Result<(), ActorRefErr> {
        let (tx, rx) = oneshot::channel();

        if let Err(e) = self.client.notify(BeginHandshake {
            request_id,
            seed_nodes: seed_nodes.clone(),
            on_handshake_complete: tx,
        }) {
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
            time_taken_millis: now.elapsed().as_millis() as u64,
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

    async fn stopped(&mut self, ctx: &mut ActorContext) {
        match &mut self.state {
            None => {}
            Some(state) => match state {
                ClientState::Idle { .. } => {}
                ClientState::Connected(connection) => {
                    if ctx.system().is_terminated() {
                        debug!(
                            "system shutdown, notifying node(addr={}, id={:?})",
                            &self.addr, &self.node_id
                        );

                        let ping_event = SessionEvent::Ping(PingEvent {
                            message_id: Uuid::new_v4().to_string(),
                            node_id: ctx.system().remote().node_id(),
                            system_terminated: true,
                            ..PingEvent::default()
                        });

                        let _ = write_bytes(
                            Bytes::from(ping_event.write_to_bytes().unwrap()),
                            &mut connection.write,
                        )
                        .await;
                    }

                    let _res = connection.write.close().await;
                    connection.receive_task.abort();
                }
                ClientState::Terminated { .. } => {}
            },
        }

        if let Some(ping_timer) = self.ping_timer.take() {
            ping_timer.stop();
        }

        debug!("client actor: {} stopped", &self.addr);
    }
}

pub enum ClientState {
    Idle { connection_attempts: usize },
    Connected(ConnectionState),
    Terminated,
}

impl ClientState {
    pub fn connection_attempts(&self) -> Option<usize> {
        match &self {
            ClientState::Idle {
                connection_attempts,
            } => Some(*connection_attempts),
            ClientState::Connected(_) => None,
            ClientState::Terminated => None,
        }
    }
}

pub struct ConnectionState {
    identity: NodeIdentity,
    handshake: HandshakeStatus,
    write: FramedWrite<WriteHalf<TcpStream>, LengthDelimitedCodec>,
    receive_task: JoinHandle<()>,
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

impl Display for RemoteClientErr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self {
            RemoteClientErr::Encoding => {
                write!(f, "failed to encode/decode message")
            }
            RemoteClientErr::StreamErr(e) => {
                write!(f, "stream error (error={})", e)
            }
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub enum ClientType {
    Client,
    Worker,
}

pub struct BeginHandshake {
    request_id: Uuid,
    seed_nodes: Vec<RemoteNode>,
    on_handshake_complete: Sender<()>,
}

impl Message for BeginHandshake {
    type Result = ();
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
