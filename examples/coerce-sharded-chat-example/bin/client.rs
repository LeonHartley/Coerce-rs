use clap::{arg, Command};

use coerce_sharded_chat_example::websocket::client::ChatClient;

use std::str::FromStr;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::mpsc::Receiver;
use tokio_tungstenite::tungstenite::Message;

#[macro_use]
extern crate tracing;

#[tokio::main]
pub async fn main() {
    let mut matches = Command::new("coerce-sharded-chat-client")
        .arg(arg!(--name <NAME> "The display name of the chat user tied to this connection"))
        .arg(arg!(--websocket_url <WEBSOCKET_URL> "The host and port which the sharded chat websockets will listen from"))
        .arg(arg!(--log_level [LOG_LEVEL] "The minimum level at which the application log will be filtered (default=INFO)"))
        .get_matches();

    let name = matches.remove_one::<String>("name").unwrap();
    let websocket_url = matches.remove_one::<String>("websocket_url").unwrap();

    setup_logging(matches.remove_one("log_level").unwrap_or("INFO"));

    let mut client = ChatClient::connect(&websocket_url, &name)
        .await
        .unwrap_or_else(|_| panic!("connect to websocket (url={})", &websocket_url));

    let mut current_chat = None;

    info!("connected! Join a chat stream via /join [chat-stream-name]");

    tokio::spawn(websocket_read(client.take_reader().unwrap()));

    let mut reader = BufReader::new(tokio::io::stdin()).lines();
    while let Ok(line) = reader.next_line().await {
        if let Some(line) = line {
            if line.is_empty() {
                continue;
            } else if line.starts_with('/') {
                process_command(line, &mut client, &mut current_chat).await;
            } else if let Some(chat_stream) = &current_chat {
                client.chat(chat_stream.to_string(), line).await;
            }
        }
    }
}

async fn websocket_read(mut websocket_reader: Receiver<Message>) {
    while let Some(message) = websocket_reader.recv().await {
        match message {
            Message::Binary(bytes) => {
                info!("{}", String::from_utf8(bytes).unwrap());
            }

            _ => {}
        }
    }
}

async fn process_command(line: String, client: &mut ChatClient, current_chat: &mut Option<String>) {
    let mut words = line.split(' ');
    match words.next() {
        Some("/join") => {
            let stream_name = words.next().expect("chat stream name").to_string();

            client.join_chat(stream_name.clone()).await;
            current_chat.replace(stream_name);
        }
        Some("/spam") => {
            if let Some(current_chat) = current_chat {
                let mut i = 0;
                loop {
                    client.chat(current_chat.to_string(), format!("{i}")).await;

                    i += 1;
                }
            }
        }

        _ => {
            error!("invalid command, commands available: /join <stream name>");
        }
    }
}

fn setup_logging(log_level: &str) {
    tracing_subscriber::fmt()
        .with_file(true)
        .with_line_number(true)
        .with_target(true)
        .with_thread_names(true)
        .with_ansi(false)
        .with_max_level(tracing::Level::from_str(log_level).unwrap())
        .init();
}
