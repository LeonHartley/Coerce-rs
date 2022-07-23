use clap::{arg, Command};
use coerce::remote::system::NodeId;
use coerce_sharded_chat_example::app::{ShardedChat, ShardedChatConfig, ShardedChatPersistence};
use metrics_exporter_prometheus::PrometheusBuilder;
use metrics_util::MetricKindMask;
use std::env;

use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Duration;
use tracing_subscriber::fmt::format::FmtSpan;

#[tokio::main]
pub async fn main() {
    let config = configure_application();
    let mut sharded_chat = ShardedChat::start(config).await;
    let _ = tokio::signal::ctrl_c().await;
    sharded_chat.stop().await;
}

fn configure_application() -> ShardedChatConfig {
    let matches = Command::new("coerce-sharded-chat-server")
        .version(env::var("CARGO_PKG_VERSION").unwrap_or(String::from("1")).as_str())
        .arg(arg!(--node_id <NODE_ID> "The ID this node will identify itself as").env("NODE_ID"))
        .arg(arg!(--remote_listen_addr <LISTEN_ADDR> "The host and port which Coerce will listen to connections from").env("REMOTE_LISTEN_ADDR"))
        .arg(arg!(--websocket_listen_addr <LISTEN_ADDR> "The host and port which the sharded chat websockets will listen from").env("WEBSOCKET_LISTEN_ADDR"))
        .arg(arg!(--cluster_api_listen_addr <LISTEN_ADDR> "The host and port which the Coerce cluster HTTP API will listen from").env("CLUSTER_API_LISTEN_ADDR"))
        .arg(arg!(--log_level [LOG_LEVEL] "The minimum level at which the application log will be filtered (default=INFO)").env("LOG_LEVEL"))
        .arg(arg!(--remote_seed_addr [TCP_ADDR] "(optional) The host and port which Coerce will attempt to use as a seed node").env("REMOTE_SEED_ADDR"))
        .arg(arg!(--metrics_exporter_listen_addr [TCP_ADDR] "(optional) The host and port which the prometheus metrics exporter").env("METRICS_EXPORTER_LISTEN_ADDR"))
        .arg(arg!(--redis_addr [TCP_ADDR] "(optional) Redis TCP address. By default, in-memory persistence is enabled").env("REDIS_ADDR"))
        .get_matches();

    let node_id = matches
        .value_of("node_id")
        .unwrap()
        .parse::<NodeId>()
        .expect("invalid node_id (unsigned 64bit integer)");

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

    if let Some(metrics_exporter_listen_addr) = matches.value_of("metrics_exporter_listen_addr") {
        let prometheus_builder = PrometheusBuilder::new();
        prometheus_builder
            .idle_timeout(
                MetricKindMask::COUNTER | MetricKindMask::HISTOGRAM,
                Some(Duration::from_secs(60 * 30)),
            )
            .with_http_listener(
                SocketAddr::from_str(metrics_exporter_listen_addr)
                    .expect("valid metrics_exporter_listen_addr"),
            )
            .add_global_label("node_id", node_id.to_string())
            .install()
            .expect("failed to install Prometheus recorder");
    }

    tracing_subscriber::fmt()
        .compact()
        .with_max_level(tracing::Level::from_str(log_level).unwrap())
        .with_span_events(FmtSpan::NONE)
        .init();

    let persistence = if let Some(redis_addr) = matches.value_of("redis_addr") {
        ShardedChatPersistence::Redis {
            host: Some(redis_addr.to_string()),
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
