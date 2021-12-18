use super::proto::network;
use crate::remote::actor::RemoteResponse;
use crate::remote::cluster::node::RemoteNodeState;
use crate::remote::net::codec::NetworkCodec;
use crate::remote::net::message::{ClientEvent, SessionEvent};
use crate::remote::net::proto::network::{Pong, RemoteNode, SessionHandshake};
use crate::remote::net::{receive_loop, StreamData, StreamReceiver};
use crate::remote::system::{NodeId, RemoteActorSystem};
use crate::remote::tracing::extract_trace_identifier;
use chrono::format::Numeric::Timestamp;
use chrono::{DateTime, NaiveDateTime, Utc};
use futures::SinkExt;
use protobuf::well_known_types::Int64Value;
use protobuf::Message;
use std::str::FromStr;
use tokio::io::WriteHalf;
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio_util::codec::{FramedRead, FramedWrite};
use uuid::Uuid;

pub struct RemoteClient {
    pub node_id: NodeId,
    pub node_tag: String,
    pub node_started_at: DateTime<Utc>,
    write: FramedWrite<WriteHalf<TcpStream>, NetworkCodec>,
    stop: Option<oneshot::Sender<bool>>,
}

pub struct HandshakeAcknowledge {
    node_id: NodeId,
    node_tag: String,
    node_started_at: DateTime<Utc>,
}

pub struct ClientMessageReceiver {
    node_id: Option<NodeId>,
    handshake_tx: Option<oneshot::Sender<HandshakeAcknowledge>>,
}

impl ClientMessageReceiver {
    pub fn new(
        node_id: Option<NodeId>,
        handshake_tx: oneshot::Sender<HandshakeAcknowledge>,
    ) -> ClientMessageReceiver {
        let handshake_tx = Some(handshake_tx);
        Self {
            node_id,
            handshake_tx,
        }
    }
}

#[async_trait]
impl StreamReceiver for ClientMessageReceiver {
    type Message = ClientEvent;

    async fn on_receive(&mut self, msg: ClientEvent, sys: &RemoteActorSystem) {
        match msg {
            ClientEvent::Handshake(msg) => {
                let node_id = msg.node_id;

                let nodes = msg
                    .nodes
                    .into_iter()
                    .filter(|n| n.node_id != node_id)
                    .map(|n| crate::remote::cluster::node::RemoteNode {
                        id: n.get_node_id(),
                        addr: n.addr,
                        node_started_at: n.node_started_at.into_option().map(timestamp_to_datetime),
                    })
                    .collect();

                sys.notify_register_nodes(nodes);

                self.node_id = Some(node_id);

                if let Some(handshake_tx) = self.handshake_tx.take() {
                    let node_tag = msg.node_tag;
                    let node_started_at = msg
                        .node_started_at
                        .into_option()
                        .map_or_else(|| Utc::now(), timestamp_to_datetime);

                    if !handshake_tx
                        .send(HandshakeAcknowledge {
                            node_id,
                            node_tag,
                            node_started_at,
                        })
                        .is_ok()
                    {
                        warn!(target: "RemoteClient", "error sending handshake_tx");
                    }
                }
            }
            ClientEvent::Result(res) => {
                match sys.pop_request(Uuid::from_str(&res.message_id).unwrap()) {
                    Some(res_tx) => {
                        let _ = res_tx.send(RemoteResponse::Ok(res.result));
                    }
                    None => {
                        trace!(target: "RemoteClient", "node_tag={}, node_id={}, received unknown request result (id={})", sys.node_tag(), sys.node_id(), res.message_id);
                    }
                }
            }
            ClientEvent::Err(_e) => {}
            ClientEvent::Ping(_ping) => {}
            ClientEvent::Pong(pong) => {
                match sys.pop_request(Uuid::from_str(&pong.message_id).unwrap()) {
                    Some(res_tx) => res_tx
                        .send(RemoteResponse::Ok(
                            Pong {
                                message_id: pong.message_id,
                                ..Pong::default()
                            }
                            .write_to_bytes()
                            .expect("serialised pong"),
                        ))
                        .expect("send ping ok"),
                    None => {
                        //                                          :P
                        warn!(target: "RemoteClient", "received unsolicited pong");
                    }
                }
            }
        }
    }

    async fn on_close(&mut self, sys: &RemoteActorSystem) {
        if let Some(node_id) = self.node_id.take() {
            sys.deregister_client(node_id).await;
        }
    }
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

impl RemoteClient {
    pub async fn connect(
        addr: String,
        remote_node_id: Option<NodeId>,
        system: RemoteActorSystem,
        nodes: Option<Vec<RemoteNodeState>>,
        client_type: ClientType,
    ) -> Result<RemoteClient, tokio::io::Error> {
        let span = tracing::trace_span!("RemoteClient::connect", address = addr.as_str());
        let _enter = span.enter();
        let stream = TcpStream::connect(addr).await?;
        let (read, write) = tokio::io::split(stream);

        let read = FramedRead::new(read, NetworkCodec);
        let mut write = FramedWrite::new(write, NetworkCodec);

        let (stop_tx, stop_rx) = oneshot::channel();
        let (handshake_tx, handshake_rx) = oneshot::channel();
        let node_id = system.node_id();
        let node_tag = system.node_tag().to_string();

        trace!("requesting nodes");

        let nodes = match nodes {
            Some(n) => n,
            None => system.get_nodes().await,
        };

        trace!("got nodes {:?}", &nodes);

        tokio::spawn(receive_loop(
            system,
            read,
            stop_rx,
            ClientMessageReceiver::new(remote_node_id, handshake_tx),
        ));

        trace!("writing handshake");

        let trace_id = extract_trace_identifier(&span);
        let mut msg = SessionHandshake {
            node_id,
            node_tag,
            token: vec![],
            client_type: client_type.into(),
            trace_id,
            ..SessionHandshake::default()
        };

        for node in nodes {
            let node = RemoteNode {
                node_id: node.id,
                addr: node.addr,
                node_started_at: node
                    .node_started_at
                    .as_ref()
                    .map(datetime_to_timestamp)
                    .into(),
                ..RemoteNode::default()
            };

            msg.nodes.push(node);
        }

        write_msg(SessionEvent::Handshake(msg), &mut write)
            .await
            .expect("write handshake");

        trace!("waiting for handshake ack");
        let handshake_ack = handshake_rx.await.expect("handshake ack");
        let node_id = handshake_ack.node_id;
        let node_tag = handshake_ack.node_tag;
        let node_started_at = handshake_ack.node_started_at;

        trace!("handshake ack (node_id={}, node_tag={})", node_id, node_tag);
        Ok(RemoteClient {
            write,
            node_id,
            node_tag,
            node_started_at,
            stop: Some(stop_tx),
        })
    }

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
pub trait RemoteClientStream {
    async fn send(&mut self, message: SessionEvent) -> Result<(), RemoteClientErr>;
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

impl From<network::ClientType> for ClientType {
    fn from(client_type: network::ClientType) -> Self {
        match client_type {
            network::ClientType::Client => Self::Client,
            network::ClientType::Worker => Self::Worker,
        }
    }
}

impl From<ClientType> for network::ClientType {
    fn from(client_type: ClientType) -> Self {
        match client_type {
            ClientType::Client => Self::Client,
            ClientType::Worker => Self::Worker,
        }
    }
}

pub fn datetime_to_timestamp(date_time: &DateTime<Utc>) -> protobuf::well_known_types::Timestamp {
    let mut t = protobuf::well_known_types::Timestamp::new();

    t.set_seconds(date_time.timestamp());
    t.set_nanos(date_time.timestamp_subsec_nanos() as i32);
    t
}

pub fn timestamp_to_datetime(timestamp: protobuf::well_known_types::Timestamp) -> DateTime<Utc> {
    DateTime::<Utc>::from_utc(
        NaiveDateTime::from_timestamp(timestamp.seconds, timestamp.nanos as u32),
        Utc,
    )
}
