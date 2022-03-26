use crate::actor::peer::Peer;
use crate::actor::stream::{ChatMessage, ChatStream, ChatStreamFactory, Handshake};
use coerce::actor::message::{EnvelopeType, Message};
use coerce::actor::system::ActorSystem;
use coerce::actor::IntoActor;
use coerce::remote::cluster::sharding::Sharding;
use futures_util::{SinkExt, StreamExt};
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio_tungstenite::accept_async;
use tungstenite::{
    Error::{ConnectionClosed, Protocol, Utf8},
    Message as WebSocketMessage,
};
use uuid::Uuid;

pub mod client;

async fn handle_connection(
    peer_addr: IpAddr,
    stream: TcpStream,
    actor_system: ActorSystem,
    sharding: Sharding<ChatStreamFactory>,
) -> Result<(), tungstenite::Error> {
    let stream = accept_async(stream).await;
    if let Ok(stream) = stream {
        let (mut writer, mut reader) = stream.split();
        let handshake = reader.next().await;
        if let Some(Ok(handshake)) = handshake {
            let handshake: Handshake =
                Handshake::from_remote_envelope(handshake.into_data()).unwrap();

            Peer::new(handshake.name, peer_addr, reader, writer, sharding)
                .into_actor(None, &actor_system)
                .await;
        } else {
            info!("nope");
        }
    }
    Ok(())
}

async fn accept_connection(
    peer: IpAddr,
    stream: TcpStream,
    system: ActorSystem,
    sharding: Sharding<ChatStreamFactory>,
) {
    if let Err(e) = handle_connection(peer, stream, system, sharding).await {
        match e {
            ConnectionClosed | Protocol(_) | Utf8 => (),
            _err => {}
        }
    }
}

pub async fn start<S: ToSocketAddrs>(
    addr: S,
    system: ActorSystem,
    sharding: Sharding<ChatStreamFactory>,
) {
    let mut listener = TcpListener::bind(addr).await.expect("websocket listen");

    while let Ok((stream, _)) = listener.accept().await {
        let peer_address = stream.peer_addr().unwrap().ip();

        debug!("websocket connection from peer_addr: {}", peer_address);
        tokio::spawn(accept_connection(
            peer_address,
            stream,
            system.clone(),
            sharding.clone(),
        ));
    }
}
