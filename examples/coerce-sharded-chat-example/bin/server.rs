use clap::{arg, Command};
use coerce::remote::system::NodeId;
use coerce_sharded_chat_example::app::{ShardedChat, ShardedChatConfig, ShardedChatPersistence};

use std::str::FromStr;

use tracing_subscriber::fmt::format::FmtSpan;

#[tokio::main]
pub async fn main() {
    let config = configure_application();
    let mut sharded_chat = ShardedChat::start(config).await;
    let _ = tokio::signal::ctrl_c().await;
    sharded_chat.stop().await;
}

fn configure_application() -> ShardedChatConfig {
    let mut matches = Command::new("coerce-sharded-chat-server")
        .arg(arg!(--node_id <NODE_ID> "The ID this node will identify itself as").env("NODE_ID"))
        .arg(arg!(--remote_listen_addr <LISTEN_ADDR> "The host and port which Coerce will listen to connections from").env("REMOTE_LISTEN_ADDR"))
        .arg(arg!(--websocket_listen_addr <LISTEN_ADDR> "The host and port which the sharded chat websockets will listen from").env("WEBSOCKET_LISTEN_ADDR"))
        .arg(arg!(--cluster_api_listen_addr <LISTEN_ADDR> "The host and port which the Coerce cluster HTTP API will listen from").env("CLUSTER_API_LISTEN_ADDR"))
        .arg(arg!(--log_level [LOG_LEVEL] "The minimum level at which the application log will be filtered (default=INFO)").env("LOG_LEVEL"))
        .arg(arg!(--remote_seed_addr [TCP_ADDR] "(optional) The host and port which Coerce will attempt to use as a seed node").env("REMOTE_SEED_ADDR"))
        .arg(arg!(--redis_addr [TCP_ADDR] "(optional) Redis TCP address. By default, in-memory persistence is enabled").env("REDIS_ADDR"))
        .get_matches();

    let node_id: NodeId = matches
        .remove_one::<String>("node_id")
        .unwrap()
        .parse()
        .expect("invalid node_id (unsigned 64bit integer)");

    let remote_listen_addr = matches.remove_one::<String>("remote_listen_addr").unwrap();

    let websocket_listen_addr = matches
        .remove_one::<String>("websocket_listen_addr")
        .unwrap();

    let cluster_api_listen_addr = matches
        .remove_one::<String>("cluster_api_listen_addr")
        .unwrap();

    let remote_seed_addr = matches.remove_one::<String>("remote_seed_addr");

    let log_level = matches
        .remove_one::<String>("log_level")
        .unwrap_or_else(|| "INFO".to_string());

    tracing_subscriber::fmt()
        .compact()
        .with_max_level(tracing::Level::from_str(&log_level).unwrap())
        .with_span_events(FmtSpan::NONE)
        .init();

    let persistence = if let Some(redis_addr) = matches.remove_one::<String>("redis_addr") {
        ShardedChatPersistence::Redis {
            host: Some(redis_addr),
        }
    } else {
        ShardedChatPersistence::InMemory
    };

    ShardedChatConfig {
        node_id,
        remote_listen_addr,
        remote_seed_addr,
        websocket_listen_addr,
        cluster_api_listen_addr,
        persistence,
    }
}
