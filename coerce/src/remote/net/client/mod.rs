use crate::actor::context::{ActorContext, ActorStatus};
use crate::actor::message::{Handler, Message};
use crate::actor::system::ActorSystem;
use crate::actor::{Actor, ActorId, BoxedActorRef, IntoActor, LocalActorRef};
use crate::remote::net::client::connect::{Connect, Disconnected};
use crate::remote::net::codec::NetworkCodec;
use crate::remote::net::message::SessionEvent;
use crate::remote::net::proto::network as proto;
use crate::remote::net::StreamData;
use crate::remote::system::{NodeId, RemoteActorSystem};
use chrono::{DateTime, Utc};
use futures::SinkExt;
use std::collections::VecDeque;
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
    state: Option<ClientState>,
    stop: Option<oneshot::Sender<bool>>,
    write_buffer_bytes_total: usize,
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
            state: Some(ClientState::Idle {
                connection_attempts: 0,
            }),
            write_buffer: VecDeque::new(),
            write_buffer_bytes_total: 0,
        }
        .into_actor(actor_id, system.actor_system())
        .await
        .unwrap())
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
        ctx: &mut ActorContext,
    ) -> Result<(), RemoteClientErr> {
        self.write(message.0, ctx).await
    }
}

impl RemoteClient {
    pub async fn flush_buffered_writes(&mut self) {
        let mut connection_state = match &mut self.state {
            Some(ClientState::Connected(connection_state)) => connection_state,
            _ => return,
        };

        debug!(
            "flushing {} pending messages (addr={})",
            self.write_buffer.len(),
            &self.addr
        );

        while let Some(buffered_message) = self.write_buffer.pop_front() {
            let len = buffered_message.len();
            if let Ok(()) = write_bytes(&buffered_message, &mut connection_state.write).await {
                self.write_buffer_bytes_total -= len;
            } else {
                self.write_buffer.push_front(buffered_message);

                // write failed, no point trying again - break and reconnect/retry later
                break;
            }
        }
    }

    pub fn buffer_message(&mut self, message_bytes: Vec<u8>) {
        self.write_buffer_bytes_total += message_bytes.len();
        self.write_buffer.push_back(message_bytes);
    }

    pub async fn write<M: StreamData>(
        &mut self,
        message: M,
        ctx: &mut ActorContext,
    ) -> Result<(), RemoteClientErr>
    where
        M: Sync + Send,
    {
        if let Some(bytes) = message.write_to_bytes() {
            let mut buffer_message = None;

            let stream_write_error = match &mut self.state.as_mut().unwrap() {
                ClientState::Idle { .. } | ClientState::Quarantined { .. } => {
                    buffer_message = Some(bytes);

                    debug!("attempt to write to addr={} but no connection is established, buffering message (total_buffered={})",
                        &self.addr,
                        self.write_buffer.len()
                    );

                    false
                }

                ClientState::Connected(state) => {
                    if let Err(e) = write_bytes(&bytes, &mut state.write).await {
                        match e {
                            RemoteClientErr::StreamErr(_e) => {
                                warn!("node {} (addr={}) is unreachable but marked as connected, buffering message (total_buffered={})",
                                    &state.node_id,
                                    &self.addr,
                                    self.write_buffer.len());

                                buffer_message = Some(bytes);

                                true
                            }
                            _ => false,
                        }
                    } else {
                        false
                    }
                }
            };

            if let Some(message_bytes) = buffer_message {
                self.buffer_message(message_bytes);
            }

            if stream_write_error {
                self.handle(Disconnected, ctx).await;
            }
        } else {
            return Err(RemoteClientErr::Encoding);
        }

        Ok(())
    }

    pub fn close(&mut self) -> bool {
        if let Some(stop) = self.stop.take() {
            stop.send(true).is_ok()
        } else {
            false
        }
    }
}

impl ConnectionState {
    pub async fn disconnected(&mut self) {}
}

pub(crate) async fn write_bytes(
    bytes: &Vec<u8>,
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
