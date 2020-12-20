use crate::remote::net::codec::NetworkCodec;
use crate::remote::net::{receive_loop, StreamMessage, StreamReceiver};
use crate::remote::system::RemoteActorSystem;

use crate::remote::actor::RemoteResponse;
use crate::remote::net::message::{ClientEvent, SessionEvent};
use futures::SinkExt;
use serde::Serialize;

use crate::remote::net::proto::protocol::{RemoteNode, SessionHandshake};
use std::str::FromStr;
use tokio_util::codec::{FramedRead, FramedWrite};
use uuid::Uuid;

pub struct RemoteClient {
    pub node_id: Uuid,
    write: FramedWrite<tokio::io::WriteHalf<tokio::net::TcpStream>, NetworkCodec>,
    stop: Option<tokio::sync::oneshot::Sender<bool>>,
}

pub struct ClientMessageReceiver {
    handshake_tx: Option<tokio::sync::oneshot::Sender<Uuid>>,
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

                ctx.notify_register_nodes(nodes).await;

                if let Some(handshake_tx) = self.handshake_tx.take() {
                    if !handshake_tx.send(Uuid::from_str(&node_id).unwrap()).is_ok() {
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
                    warn!(target: "RemoteClient", "received unknown request result");
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

impl RemoteClient {
    pub async fn connect(
        addr: String,
        mut system: RemoteActorSystem,
        nodes: Option<Vec<crate::remote::cluster::node::RemoteNode>>,
    ) -> Result<RemoteClient, tokio::io::Error> {
        let stream = tokio::net::TcpStream::connect(addr).await?;
        let (read, write) = tokio::io::split(stream);

        let read = FramedRead::new(read, NetworkCodec);
        let mut write = FramedWrite::new(write, NetworkCodec);

        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
        let (handshake_tx, handshake_rx) = tokio::sync::oneshot::channel();
        let node_id = system.node_id();

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
            ClientMessageReceiver {
                handshake_tx: Some(handshake_tx),
            },
        ));

        trace!("writing handshake");

        let mut msg = SessionHandshake {
            node_id: node_id.to_string(),
            token: vec![],
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

        trace!("waiting for id");
        let node_id = handshake_rx.await.expect("handshake node_id");

        trace!("recv id");
        Ok(RemoteClient {
            write,
            node_id,
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
    write: &mut FramedWrite<tokio::io::WriteHalf<tokio::net::TcpStream>, NetworkCodec>,
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
