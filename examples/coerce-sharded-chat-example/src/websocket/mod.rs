use crate::actor::peer::Peer;
use futures_util::StreamExt;
use log::info;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio_tungstenite::accept_async;
use tungstenite::Error::{ConnectionClosed, Protocol, Utf8};

async fn handle_connection(peer: IpAddr, stream: TcpStream) -> Result<(), tungstenite::Error> {
    let stream = accept_async(stream).await;
    if let Ok(stream) = stream {
        let (writer, mut reader) = stream.split();
        let handshake = reader.next().await;
        if let Some(Ok(handshake)) = handshake {
            info!("handshake!");
        } else {
            info!("nope");
        }
    }
    Ok(())
}

async fn accept_connection(peer: IpAddr, stream: TcpStream) {
    if let Err(e) = handle_connection(peer, stream).await {
        match e {
            ConnectionClosed | Protocol(_) | Utf8 => (),
            _err => {}
        }
    }
}

pub async fn start<S: ToSocketAddrs>(addr: S) {
    let mut listener = TcpListener::bind(addr).await.expect("websocket listen");

    while let Ok((stream, _)) = listener.accept().await {
        let peer_address = stream.peer_addr().unwrap().ip();

        println!("peer_addr: {}", peer_address);
        tokio::spawn(accept_connection(peer_address, stream));
    }
}
