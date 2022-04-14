use chrono::Local;
use clap::{arg, Command};
use coerce_sharded_chat_example::actor::peer::{JoinChat, SendChatMessage};
use coerce_sharded_chat_example::actor::stream::ChatMessage;
use coerce_sharded_chat_example::websocket::client::ChatClient;
use futures::StreamExt;
use std::env;
use std::io::Write;
use std::str::{FromStr, Split};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::mpsc::Receiver;
use tokio_tungstenite::tungstenite::Message;

#[macro_use]
extern crate log;

#[tokio::main]
pub async fn main() {
    let matches = Command::new("coerce-sharded-chat-client")
        .version(env::var("CARGO_PKG_VERSION").unwrap_or(String::from("1")).as_str())
        .arg(arg!(--name <NAME> "The display name of the chat user tied to this connection"))
        .arg(arg!(--websocket_url <WEBSOCKET_URL> "The host and port which the sharded chat websockets will listen from"))
        .arg(arg!(--log_level [LOG_LEVEL] "The minimum level at which the application log will be filtered (default=INFO)"))
        .get_matches();

    let name = matches.value_of("name").unwrap().to_string();
    let websocket_url = matches.value_of("websocket_url").unwrap().to_string();

    setup_logging(matches.value_of("log_level").unwrap_or("INFO"));

    let mut client = ChatClient::connect(&websocket_url, &name)
        .await
        .expect(&format!("connect to websocket (url={})", &websocket_url));

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

        _ => {
            error!("invalid command, commands available: /join <stream name>");
        }
    }
}

fn setup_logging(log_level: &str) {
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
        .filter(
            None,
            log::LevelFilter::from_str(log_level).expect("valid log level"),
        )
        .try_init();
}
