use chrono::Local;
use coerce::actor::message::{EnvelopeType, Message};
use coerce_sharded_chat_example::actor::stream::{ChatMessage, Handshake};
use coerce_sharded_chat_example::app::{ShardedChat, ShardedChatConfig};
use futures_util::StreamExt;
use log::LevelFilter;
use std::io::Write;
use tokio::io::split;
use tungstenite::{connect, Message as WebSocketMessage, Result};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
pub async fn test_sharded_chat_join() {
    logger();

    let sharded_chat_config = ShardedChatConfig {
        node_id: 1,
        remote_listen_addr: "0.0.0.0:31101".to_string(),
        remote_seed_addr: None,
        websocket_listen_addr: "0.0.0.0:31102".to_string(),
    };

    let sharded_chat_config_2 = ShardedChatConfig {
        node_id: 2,
        remote_listen_addr: "0.0.0.0:32101".to_string(),
        remote_seed_addr: None,
        websocket_listen_addr: "0.0.0.0:32102".to_string(),
    };

    let sharded_chat_1 = ShardedChat::start(sharded_chat_config).await;
    let sharded_chat_2 = ShardedChat::start(sharded_chat_config_2).await;

    let client_a = websocket_client("ws://localhost:31102", "client-a").await;
    let client_b = websocket_client("ws://localhost:32102", "client-b").await;
}

async fn websocket_client(url: &'static str, name: &str) -> Result<()> {
    let (mut socket, _) = connect(url)?;

    let message = Handshake {
        name: name.to_string(),
    }
    .into_envelope(EnvelopeType::Remote)
    .unwrap()
    .into_bytes();

    let (mut read, write) = split(socket);
    let res = socket.write_message(WebSocketMessage::Binary(message));

    log::info!("send_msg = {}", res.is_ok());

    tokio::spawn(async move {
        loop {
            match read.next().await {
                Ok(msg) => match msg {
                    WebSocketMessage::Text(_) => {
                        log::info!("hi2");
                    }
                    WebSocketMessage::Binary(_) => {
                        log::info!("hi");
                    }
                    WebSocketMessage::Ping(_) => {}
                    WebSocketMessage::Pong(_) => {}
                    WebSocketMessage::Close(_) => {}
                },
                Err(_) => {}
            }
        }
    });
}

fn logger() {
    let _ = env_logger::Builder::new()
        .format(|buf, record| {
            writeln!(
                buf,
                "{} [{}] {} - {}",
                Local::now().format("%Y-%m-%dT%H:%M:%S%.6f"),
                record.level(),
                record.target(),
                record.args(),
            )
        })
        .filter(None, LevelFilter::Trace)
        .try_init();
}
