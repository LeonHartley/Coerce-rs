use crate::remote::codec::MessageCodec;
use crate::remote::context::RemoteActorSystem;
use crate::remote::net::codec::NetworkCodec;
use crate::remote::net::{receive_loop, StreamReceiver};

use crate::remote::actor::RemoteResponse;
use crate::remote::cluster::node::RemoteNode;
use crate::remote::net::message::{ClientEvent, SessionEvent, SessionHandshake};
use futures::SinkExt;
use serde::Serialize;

use tokio_util::codec::{FramedRead, FramedWrite};
use uuid::Uuid;

pub struct RemoteClient<C: MessageCodec> {
    pub node_id: Uuid,
    codec: C,
    write: FramedWrite<tokio::io::WriteHalf<tokio::net::TcpStream>, NetworkCodec>,
    stop: Option<tokio::sync::oneshot::Sender<bool>>,
}

pub struct ClientMessageReceiver {
    handshake_tx: Option<tokio::sync::oneshot::Sender<Uuid>>,
}

#[async_trait]
impl StreamReceiver<ClientEvent> for ClientMessageReceiver {
    async fn on_recv(&mut self, msg: ClientEvent, ctx: &mut RemoteActorSystem) {
        match msg {
            ClientEvent::Handshake(msg) => {
                info!("{}, {:?}", ctx.node_id(), &msg.nodes);

                let node_id = msg.node_id;
                let nodes = msg.nodes.into_iter().filter(|n| n.id != node_id).collect();

                ctx.notify_register_nodes(nodes).await;

                if let Some(handshake_tx) = self.handshake_tx.take() {
                    if !handshake_tx.send(msg.node_id).is_ok() {
                        warn!(target: "RemoteClient", "error sending handshake_tx");
                    }
                }
            }
            ClientEvent::Result(id, res) => match ctx.pop_request(id).await {
                Some(res_tx) => {
                    let _ = res_tx.send(RemoteResponse::Ok(res));
                }
                None => {
                    warn!(target: "RemoteClient", "received unknown request result");
                }
            },
            ClientEvent::Err(_id, _err) => {}
            ClientEvent::Ping(_id) => {}
            ClientEvent::Pong(id) => match ctx.pop_request(id).await {
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

impl<C: MessageCodec> RemoteClient<C> {
    pub async fn connect(
        addr: String,
        mut context: RemoteActorSystem,
        mut codec: C,
        nodes: Option<Vec<RemoteNode>>,
    ) -> Result<RemoteClient<C>, tokio::io::Error>
    where
        C: 'static + Sync + Send,
    {
        let stream = tokio::net::TcpStream::connect(addr).await?;
        let (read, write) = tokio::io::split(stream);

        let read = FramedRead::new(read, NetworkCodec);
        let mut write = FramedWrite::new(write, NetworkCodec);

        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
        let (handshake_tx, handshake_rx) = tokio::sync::oneshot::channel();
        let node_id = context.node_id();

        trace!("requesting nodes");

        let nodes = match nodes {
            Some(n) => n,
            None => context.get_nodes().await,
        };

        trace!("got nodes {:?}", &nodes);

        tokio::spawn(receive_loop(
            context,
            read,
            stop_rx,
            ClientMessageReceiver {
                handshake_tx: Some(handshake_tx),
            },
            codec.clone(),
        ));

        trace!("writing handshake");
        write_msg(
            SessionEvent::Handshake(SessionHandshake {
                node_id,
                nodes,
                token: vec![],
            }),
            &mut codec,
            &mut write,
        )
        .await
        .expect("write handshake");

        trace!("waiting for id");
        let node_id = handshake_rx.await.expect("handshake node_id");

        trace!("recv id");
        Ok(RemoteClient {
            write,
            codec,
            node_id,
            stop: Some(stop_tx),
        })
    }

    pub async fn write<M: Serialize>(&mut self, message: M) -> Result<(), RemoteClientErr>
    where
        C: 'static + Sync + Send,
        M: Sync + Send,
    {
        write_msg(message, &mut self.codec, &mut self.write).await
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
impl<C: MessageCodec> RemoteClientStream for RemoteClient<C>
where
    C: 'static + Sync + Send,
{
    async fn send(&mut self, message: SessionEvent) -> Result<(), RemoteClientErr> {
        self.write(message).await
    }
}

async fn write_msg<M: Serialize, C: MessageCodec>(
    message: M,
    codec: &mut C,
    write: &mut FramedWrite<tokio::io::WriteHalf<tokio::net::TcpStream>, NetworkCodec>,
) -> Result<(), RemoteClientErr>
where
    C: Sync + Send,
    M: Sync + Send,
{
    match codec.encode_msg(message) {
        Some(message) => match write.send(message).await {
            Ok(()) => Ok(()),
            Err(e) => Err(RemoteClientErr::StreamErr(e)),
        },
        None => Err(RemoteClientErr::Encoding),
    }
}
