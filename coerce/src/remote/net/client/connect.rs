use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message};
use crate::actor::{IntoActor, LocalActorRef};
use crate::remote::cluster::node::RemoteNodeState;
use crate::remote::net::client::receive::ClientMessageReceiver;
use crate::remote::net::client::{write_msg, ClientType, ConnectionState, RemoteClient, ClientState};
use crate::remote::net::codec::NetworkCodec;
use crate::remote::net::message::{datetime_to_timestamp, SessionEvent};
use crate::remote::net::proto::network::{RemoteNode, SessionHandshake};
use crate::remote::net::receive_loop;
use crate::remote::system::{NodeId, RemoteActorSystem};
use crate::remote::tracing::extract_trace_identifier;
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio_util::codec::{FramedRead, FramedWrite};

pub struct Connect {
    addr: String,
    remote_node_id: Option<NodeId>,
}

pub struct Disconnected;

#[async_trait]
impl Handler<Connect> for RemoteClient {
    async fn handle(&mut self, message: Connect, ctx: &mut ActorContext) -> bool {
        if let Some(connection_state) = self.connect(message, ctx).await {
            self.state = ClientState::Connected(connection_state);

            true
        } else {

            // TODO: enqueue another connection attempt
            false
        }
    }
}

impl Handler<Disconnected> for RemoteClient {
    async fn handle(&mut self, message: Disconnected, ctx: &mut ActorContext) {
        // TODO: try to connect again, if fails after {n} attempts with a timeout,
        //       we should quarantine the node and ensuring the node no longer
        //       participates in cluster activities/sharding
    }
}

impl Connect {
    pub fn new(
        addr: String,
        remote_node_id: Option<NodeId>,
    ) -> Self {
        Self {
            addr,
            remote_node_id,
        }
    }
}

impl Message for Connect { type Result = bool; }

impl RemoteClient {
    async fn connect(&self, connect: Connect, ctx: &mut ActorContext) -> Option<ConnectionState> {
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

        let receive_task = tokio::spawn(receive_loop(
            system.clone(),
            read,
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
                tag: node.tag,
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

        Some(ConnectionState {
            node_id,
            node_tag,
            node_started_at,
            write,
            receive_task,
        })
    }
}