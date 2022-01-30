use chrono::Local;
use coerce_sharded_chat_example::app::{ShardedChat, ShardedChatConfig};
use log::LevelFilter;
use std::io::Write;

#[tokio::main]
pub async fn main() {
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

    let sharded_chat_config = ShardedChatConfig {
        node_id: 1,
        remote_listen_addr: "0.0.0.0:31101".to_string(),
        remote_seed_addr: None,
        websocket_listen_addr: "0.0.0.0:31102".to_string(),
        cluster_api_listen_addr: "0.0.0.0:31111".to_string(),
    };

    let mut sharded_chat = ShardedChat::start(sharded_chat_config).await;
    let _ = tokio::signal::ctrl_c().await;
    sharded_chat.stop().await;
}
