pub mod client;

#[macro_use]
extern crate log;

use crate::client::ChatClient;
use chrono::Local;
use coerce_sharded_chat_example::actor::peer::{JoinChat, SendChatMessage};
use coerce_sharded_chat_example::actor::stream::ChatMessage;
use coerce_sharded_chat_example::app::{ShardedChat, ShardedChatConfig};
use log::LevelFilter;
use std::io::Write;
use std::time::Duration;
use tokio::signal::ctrl_c;
use tokio::time::sleep;

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
pub async fn test_sharded_chat_join_and_chat() {
    logger();

    let sharded_chat_config = ShardedChatConfig {
        node_id: 1,
        remote_listen_addr: "localhost:31101".to_string(),
        remote_seed_addr: None,
        websocket_listen_addr: "localhost:31102".to_string(),
        cluster_api_listen_addr: "0.0.0.0:3000".to_string(),
    };

    let sharded_chat_config_2 = ShardedChatConfig {
        node_id: 2,
        remote_listen_addr: "localhost:32101".to_string(),
        remote_seed_addr: Some("localhost:31101".to_string()),
        websocket_listen_addr: "localhost:32102".to_string(),
        cluster_api_listen_addr: "0.0.0.0:32103".to_string(),
    };

    let _sharded_chat_1 = ShardedChat::start(sharded_chat_config).await;
    let _sharded_chat_2 = ShardedChat::start(sharded_chat_config_2).await;

    // tokio::time::sleep(Duration::from_secs(1)).await;

    let mut client_a = ChatClient::connect("ws://localhost:31102", "client-a")
        .await
        .expect("connection 1");

    let mut client_b = ChatClient::connect("ws://localhost:32102", "client-b")
        .await
        .expect("connection 2");

    client_a
        .write(
            0,
            JoinChat {
                stream_name: "example-chat-stream".to_string(),
                join_token: None,
            },
        )
        .await;

    client_b
        .write(
            0,
            JoinChat {
                stream_name: "example-chat-stream".to_string(),
                join_token: None,
            },
        )
        .await;

    log::info!("reading chatmsgs");
    let welcome_message_a = client_a.read::<ChatMessage>().await.unwrap();
    let welcome_message_b = client_b.read::<ChatMessage>().await.unwrap();

    assert_eq!(
        "Welcome to example-chat-stream, say hello!",
        &welcome_message_a.message
    );
    assert_eq!(
        "Welcome to example-chat-stream, say hello!",
        &welcome_message_b.message
    );

    log::info!("asserted welcome msgs");

    client_b
        .write(
            1,
            SendChatMessage {
                chat_stream: "example-chat-stream".to_string(),
                message: ChatMessage {
                    sender: String::default(),
                    message: "Hello!".to_string(),
                },
            },
        )
        .await;

    let received_message_a = client_a.read::<ChatMessage>().await.unwrap();
    let received_message_b = client_b.read::<ChatMessage>().await.unwrap();

    assert_eq!("Hello!", &received_message_a.message);
    assert_eq!("client-b", &received_message_a.sender);

    assert_eq!("Hello!", &received_message_b.message);
    assert_eq!("client-b", &received_message_b.sender);

    client_a
        .write(
            1,
            SendChatMessage {
                chat_stream: "example-chat-stream".to_string(),
                message: ChatMessage {
                    sender: String::default(),
                    message: "Heyo!".to_string(),
                },
            },
        )
        .await;

    let received_message_a = client_a.read::<ChatMessage>().await.unwrap();
    let received_message_b = client_b.read::<ChatMessage>().await.unwrap();

    assert_eq!("Heyo!", &received_message_a.message);
    assert_eq!("client-a", &received_message_a.sender);

    assert_eq!("Heyo!", &received_message_b.message);
    assert_eq!("client-a", &received_message_b.sender);

    let mut client_c = ChatClient::connect("ws://localhost:32102", "client-c")
        .await
        .expect("connection 3");

    client_c
        .write(
            0,
            JoinChat {
                stream_name: "example-chat-stream".to_string(),
                join_token: None,
            },
        )
        .await;

    // ensure message history is sent
    let hello = client_c.read::<ChatMessage>().await.unwrap();
    let heyo = client_c.read::<ChatMessage>().await.unwrap();

    assert_eq!("Hello!", &hello.message);
    // assert_eq!("Heyo!", &heyo.message);
    //
    // let welcome_message_c = client_c.read::<ChatMessage>().await.unwrap();
    //
    // assert_eq!(
    //     "Welcome to example-chat-stream, say hello!",
    //     &welcome_message_c.message
    // );

    // ctrl_c().await;
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
