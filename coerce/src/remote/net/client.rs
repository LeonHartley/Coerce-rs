use super::proto::protocol;
use crate::remote::actor::RemoteResponse;
use crate::remote::net::codec::NetworkCodec;
use crate::remote::net::message::{ClientEvent, SessionEvent};
use crate::remote::net::proto::protocol::{RemoteNode, SessionHandshake};
use crate::remote::net::{receive_loop, StreamMessage, StreamReceiver};
use crate::remote::system::RemoteActorSystem;
use crate::remote::tracing::extract_trace_identifier;
use futures::SinkExt;
use std::str::FromStr;
use tokio::io::WriteHalf;
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio_util::codec::{FramedRead, FramedWrite};
use uuid::Uuid;

pub struct RemoteClient {
    pub node_id: Uuid,
    pub node_tag: String,
    write: FramedWrite<WriteHalf<TcpStream>, NetworkCodec>,
    stop: Option<oneshot::Sender<bool>>,
}

pub struct HandshakeAcknowledge {
    node_id: Uuid,
    node_tag: String,
}

pub struct ClientMessageReceiver {
    handshake_tx: Option<oneshot::Sender<HandshakeAcknowledge>>,
}

impl ClientMessageReceiver {
    pub fn new(handshake_tx: oneshot::Sender<HandshakeAcknowledge>) -> ClientMessageReceiver {
        Self {
            handshake_tx: Some(handshake_tx),
        }
    }
}

#[async_trait]
impl StreamReceiver for ClientMessageReceiver {
    type Message = ClientEvent;

    async fn on_recv(&mut self, msg: ClientEvent, ctx: &mut RemoteActorSystem) {
        match msg {
            ClientEvent::Handshake(msg) => {
                trace!("{}", ctx.node_id());

                let node_id = msg.node_id;
                let nodes = msg
                    .nodes
                    .into_iter()
                    .filter(|n| n.node_id != node_id)
                    .map(|n| crate::remote::cluster::node::RemoteNode {
                        id: Uuid::from_str(n.get_node_id()).unwrap(),
                        addr: n.addr,
                    })
                    .collect();

                ctx.notify_register_nodes(nodes);

                if let Some(handshake_tx) = self.handshake_tx.take() {
                    if !handshake_tx
                        .send(HandshakeAcknowledge {
                            node_id: Uuid::from_str(&node_id).unwrap(),
                            node_tag: msg.node_tag,
                        })
                        .is_ok()
                    {
                        warn!(target: "RemoteClient", "error sending handshake_tx");
                    }
                }
            }
            ClientEvent::Result(res) => match ctx
                .pop_request(Uuid::from_str(&res.message_id).unwrap())
                .await
            {
                Some(res_tx) => {
                    let _ = res_tx.send(RemoteResponse::Ok(res.result));
                }
                None => {
                    warn!(target: "RemoteClient", "node_tag={}, node_id={}, received unknown request result (id={})", ctx.node_tag(), ctx.node_id(), res.message_id);
                }
            },
            ClientEvent::Err(_e) => {}
            ClientEvent::Ping(_ping) => {}
            ClientEvent::Pong(pong) => match ctx
                .pop_request(Uuid::from_str(&pong.message_id).unwrap())
                .await
            {
                Some(res_tx) => {
                    res_tx.send(RemoteResponse::PingOk).expect("send ping ok");
                }
                None => {
                    //                                          :P
                    warn!(target: "RemoteClient", "received unsolicited pong");
                }
            },
        }
    }

    async fn on_close(&mut self, _ctx: &mut RemoteActorSystem) {}
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
        system: RemoteActorSystem,
        nodes: Option<Vec<crate::remote::cluster::node::RemoteNode>>,
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
        let node_id = system.node_id().to_string();
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
            ClientMessageReceiver::new(handshake_tx),
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
            msg.nodes.push(RemoteNode {
                node_id: node.id.to_string(),
                addr: node.addr,
                ..RemoteNode::default()
            });
        }

        write_msg(SessionEvent::Handshake(msg), &mut write)
            .await
            .expect("write handshake");

        trace!("waiting for handshake ack");
        let handshake_ack = handshake_rx.await.expect("handshake ack");
        let node_id = handshake_ack.node_id;
        let node_tag = handshake_ack.node_tag;

        trace!("handshake ack (node_id={}, node_tag={})", node_id, node_tag);
        Ok(RemoteClient {
            write,
            node_id,
            node_tag,
            stop: Some(stop_tx),
        })
    }

    pub async fn write<M: StreamMessage>(&mut self, message: M) -> Result<(), RemoteClientErr>
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

async fn write_msg<M: StreamMessage>(
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

impl From<protocol::ClientType> for ClientType {
    fn from(client_type: protocol::ClientType) -> Self {
        match client_type {
            protocol::ClientType::Client => Self::Client,
            protocol::ClientType::Worker => Self::Worker,
        }
    }
}

impl From<ClientType> for protocol::ClientType {
    fn from(client_type: ClientType) -> Self {
        match client_type {
            ClientType::Client => Self::Client,
            ClientType::Worker => Self::Worker,
        }
    }
}
