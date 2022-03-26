use chrono::Local;
use clap::{arg, Command};
use coerce::remote::system::NodeId;
use coerce_sharded_chat_example::app::{ShardedChat, ShardedChatConfig};
use log::LevelFilter;
use std::env;
use std::io::Write;
use std::str::FromStr;

#[tokio::main]
pub async fn main() {
    let mut sharded_chat = ShardedChat::start(parse_config_from_app_args()).await;
    let _ = tokio::signal::ctrl_c().await;
    sharded_chat.stop().await;
}

fn parse_config_from_app_args() -> ShardedChatConfig {
    let matches = Command::new("coerce-sharded-chat-server")
        .version(env::var("CARGO_PKG_VERSION").unwrap_or(String::from("1")).as_str())
        .arg(arg!(--node_id <NODE_ID> "The ID this node will identify itself as"))
        .arg(arg!(--remote_listen_addr <LISTEN_ADDR> "The host and port which Coerce will listen to connections from"))
        .arg(arg!(--websocket_listen_addr <LISTEN_ADDR> "The host and port which the sharded chat websockets will listen from"))
        .arg(arg!(--cluster_api_listen_addr <LISTEN_ADDR> "The host and port which the Coerce cluster HTTP API will listen from"))
        .arg(arg!(--log_level [LOG_LEVEL] "The minimum level at which the application log will be filtered (default=INFO)"))
        .arg(arg!(--remote_seed_addr [TCP_ADDR] "(optional) The host and port which Coerce will attempt to use as a seed node"))
        .get_matches();

    let node_id = matches
        .value_of("node_id")
        .unwrap()
        .parse::<NodeId>()
        .expect("valid node_id (integer)");

    let remote_listen_addr = matches.value_of("remote_listen_addr").unwrap().to_string();

    let websocket_listen_addr = matches
        .value_of("websocket_listen_addr")
        .unwrap()
        .to_string();

    let cluster_api_listen_addr = matches
        .value_of("cluster_api_listen_addr")
        .unwrap()
        .to_string();

    let remote_seed_addr = matches.value_of("remote_seed_addr").map(|v| v.to_string());
    let log_level = matches
        .value_of("log_level")
        .map(|level| level)
        .unwrap_or("INFO");

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
            LevelFilter::from_str(log_level).expect("valid log level"),
        )
        .try_init();

    ShardedChatConfig {
        node_id,
        remote_listen_addr,
        remote_seed_addr,
        websocket_listen_addr,
        cluster_api_listen_addr,
    }
}
