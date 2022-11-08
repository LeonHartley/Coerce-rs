#[macro_use]
extern crate tracing;

use coerce_sharded_chat_example::actor::peer::{JoinChat, SendChatMessage};
use coerce_sharded_chat_example::actor::stream::ChatMessage;
use coerce_sharded_chat_example::app::{ShardedChat, ShardedChatConfig, ShardedChatPersistence};
use coerce_sharded_chat_example::websocket::client::ChatClient;

use futures_util::future::join_all;
use std::io::Write;
use std::str::FromStr;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use tracing_subscriber::fmt::format::FmtSpan;

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
pub async fn test_sharded_chat_join_and_chat() {
    logger();

    let sharded_chat_config = ShardedChatConfig {
        node_id: 1,
        remote_listen_addr: "localhost:31101".to_string(),
        remote_seed_addr: None,
        websocket_listen_addr: "localhost:31102".to_string(),
        cluster_api_listen_addr: "0.0.0.0:3000".to_string(),
        persistence: ShardedChatPersistence::InMemory,
    };

    let sharded_chat_config_2 = ShardedChatConfig {
        node_id: 2,
        remote_listen_addr: "localhost:32101".to_string(),
        remote_seed_addr: Some("localhost:31101".to_string()),
        websocket_listen_addr: "localhost:32102".to_string(),
        cluster_api_listen_addr: "0.0.0.0:32103".to_string(),
        persistence: ShardedChatPersistence::InMemory,
    };

    let _sharded_chat_1 = ShardedChat::start(sharded_chat_config).await;
    let _sharded_chat_2 = ShardedChat::start(sharded_chat_config_2).await;

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
                chat_stream_id: "example-chat-stream".to_string(),
                join_token: None,
            },
        )
        .await;

    client_b
        .write(
            0,
            JoinChat {
                chat_stream_id: "example-chat-stream".to_string(),
                join_token: None,
            },
        )
        .await;

    info!("reading chatmsgs");
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

    info!("asserted welcome msgs");

    client_b
        .write(
            1,
            SendChatMessage(ChatMessage {
                chat_stream_id: "example-chat-stream".to_string(),
                message_id: None,
                sender: None,
                message: "Hello!".to_string(),
            }),
        )
        .await;

    let received_message_a = client_a.read::<ChatMessage>().await.unwrap();
    let received_message_b = client_b.read::<ChatMessage>().await.unwrap();

    assert_eq!("Hello!", &received_message_a.message);
    assert_eq!("client-b", &received_message_a.sender.unwrap());

    assert_eq!("Hello!", &received_message_b.message);
    assert_eq!("client-b", &received_message_b.sender.unwrap());

    client_a
        .write(
            1,
            SendChatMessage(ChatMessage {
                chat_stream_id: "example-chat-stream".to_string(),
                message_id: None,
                sender: None,
                message: "Heyo!".to_string(),
            }),
        )
        .await;

    let received_message_a = client_a.read::<ChatMessage>().await.unwrap();
    let received_message_b = client_b.read::<ChatMessage>().await.unwrap();

    assert_eq!("Heyo!", &received_message_a.message);
    assert_eq!("client-a", &received_message_a.sender.unwrap());

    assert_eq!("Heyo!", &received_message_b.message);
    assert_eq!("client-a", &received_message_b.sender.unwrap());

    let mut client_c = ChatClient::connect("ws://localhost:32102", "client-c")
        .await
        .expect("connection 3");

    client_c
        .write(
            0,
            JoinChat {
                chat_stream_id: "example-chat-stream".to_string(),
                join_token: None,
            },
        )
        .await;

    // ensure message history is sent
    let hello = client_c.read::<ChatMessage>().await.unwrap();
    let heyo = client_c.read::<ChatMessage>().await.unwrap();

    assert_eq!("Hello!", &hello.message);
    assert_eq!("Heyo!", &heyo.message);

    let welcome_message_c = client_c.read::<ChatMessage>().await.unwrap();

    assert_eq!(
        "Welcome to example-chat-stream, say hello!",
        &welcome_message_c.message
    );

    // ctrl_c().await;
}

#[ignore]
#[tokio::test(flavor = "multi_thread", worker_threads = 200)]
pub async fn stress_test_sharded_chat_servers() {
    logger();

    let nodes = vec![
        "ws://localhost:31102",
        "ws://localhost:32102",
        "ws://localhost:33102",
        "ws://localhost:34102",
    ];

    let chat_stream_count = 10000;
    let connections = Arc::new(AtomicU64::new(0));

    let mut tasks = vec![];
    for (i, node) in nodes.iter().enumerate() {
        for n in 0..1000 {
            let connections = connections.clone();
            tasks.push(async move {
                connections.clone().fetch_add(1, Relaxed);

                let client_name = format!("client-{}-{}", i, n);
                let mut client = ChatClient::connect(node, &client_name)
                    .await
                    .expect("connect");

                for c in 0..chat_stream_count {
                    client
                        .write(
                            0,
                            JoinChat {
                                chat_stream_id: format!("chat-stream-{}", c),
                                join_token: None,
                            },
                        )
                        .await;
                }

                loop {
                    for c in 0..chat_stream_count {
                        client
                            .write(
                                1,
                                SendChatMessage(ChatMessage {
                                    chat_stream_id: format!("chat-stream-{}", c),
                                    message_id: None,
                                    sender: None,
                                    message: "hi".to_string(),
                                }),
                            )
                            .await;
                    }
                }
            });
        }
    }

    let _ = join_all(tasks).await;
    info!(
        "{} connections created and joined {} chat streams",
        connections.load(Relaxed),
        chat_stream_count
    );
}

pub fn logger() {
    tracing_subscriber::fmt()
        // .with_file(true)
        .compact()
        .with_target(true)
        .with_thread_names(true)
        .with_span_events(FmtSpan::NONE)
        .with_ansi(false)
        .with_max_level(
            tracing::Level::from_str(
                std::env::var("LOG_LEVEL")
                    .map_or(String::from("OFF"), |s| s)
                    .as_str(),
            )
            .unwrap(),
        )
        .init();
}
