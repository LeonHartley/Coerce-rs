use async_trait::async_trait;
use coerce::actor::message::{FromBytes, MessageUnwrapErr, MessageWrapErr, ToBytes};
use coerce::actor::system::ActorSystem;
use coerce::persistent::Persistence;
use coerce::remote::cluster::node::NodeSelector;
use coerce::remote::heartbeat::HeartbeatConfig;
use coerce::remote::net::server::RemoteServer;
use coerce::remote::system::{NodeId, RemoteActorSystem};
use coerce_replication::simple::{Error, Read, RemoteRead, Replicator};
use coerce_replication::storage::{Key, Snapshot, Storage, StorageErr, Value};
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::oneshot;
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::fmt::format::FmtSpan;

#[derive(Clone)]
struct TestKey(String);

impl FromBytes for TestKey {
    fn from_bytes(buf: Vec<u8>) -> Result<Self, MessageUnwrapErr> {
        Ok(TestKey(String::from_utf8(buf).unwrap()))
    }
}

impl ToBytes for TestKey {
    fn to_bytes(self) -> Result<Vec<u8>, MessageWrapErr> {
        Ok(self.0.into_bytes())
    }
}

impl Key for TestKey {}

impl FromBytes for TestValue {
    fn from_bytes(buf: Vec<u8>) -> Result<Self, MessageUnwrapErr> {
        Ok(TestValue(String::from_utf8(buf).unwrap()))
    }
}

impl ToBytes for TestValue {
    fn to_bytes(self) -> Result<Vec<u8>, MessageWrapErr> {
        Ok(self.0.into_bytes())
    }
}

#[derive(Clone)]
struct TestValue(String);

impl Value for TestValue {}

struct TestStorage {
    data: HashMap<String, String>,
}

struct TestSnapshot {}

impl Snapshot for TestSnapshot {}

#[async_trait]
impl Storage for TestStorage {
    type Key = TestKey;

    type Value = TestValue;

    type Snapshot = TestSnapshot;

    fn current_version(&self) -> Option<u64> {
        todo!()
    }

    async fn read(&mut self, key: Self::Key) -> Result<Self::Value, StorageErr> {
        Ok(TestValue(format!("{}-hello", key.0)))
    }

    async fn write(&mut self, key: Self::Key, value: Self::Value) -> Result<(), StorageErr> {
        todo!()
    }

    fn snapshot(&mut self) -> Result<Self::Snapshot, Error> {
        todo!()
    }
}

#[tokio::test]
pub async fn test_simple_replicator_read() {
    create_trace_logger();

    let storage_1 = TestStorage {
        data: HashMap::new(),
    };

    let storage_2 = TestStorage {
        data: HashMap::new(),
    };

    let (remote_1, server_1) = create_system("localhost:10011", 1, None).await;
    let (remote_2, server_2) = create_system("localhost:10012", 2, Some("localhost:10011")).await;

    let replicator_1 =
        Replicator::<TestStorage>::new("test-replicator", &remote_1, NodeSelector::All, storage_1)
            .await
            .unwrap();

    let replicator_2 =
        Replicator::<TestStorage>::new("test-replicator", &remote_2, NodeSelector::All, storage_2)
            .await
            .unwrap();

    let (tx, rx) = oneshot::channel();
    replicator_2
        .notify(Read {
            key: TestKey("my-key".to_string()),
            on_completion: Some(tx),
        })
        .unwrap();

    let res = rx.await.unwrap();
    match res {
        Ok(res) => {
            tracing::info!("received {} result", &res.0);
        }
        Err(e) => {
            tracing::info!("received {:?} error", &e);
        }
    }
}

async fn create_system(
    listen_addr: &str,
    node_id: NodeId,
    seed_addr: Option<&str>,
) -> (RemoteActorSystem, RemoteServer) {
    let sys = ActorSystem::new();
    let remote = RemoteActorSystem::builder()
        .with_actor_system(sys)
        .with_tag(format!("node-{node_id}"))
        .with_actors(|a| {
            a.with_handler::<Replicator<TestStorage>, RemoteRead<TestKey>>(
                "TestReplicator.RemoteRead",
            )
        })
        .with_id(node_id)
        .build()
        .await;

    let mut server = remote.clone().cluster_worker().listen_addr(listen_addr);

    if let Some(seed_addr) = seed_addr {
        server = server.with_seed_addr(seed_addr);
    }

    let server = server.start().await;
    (remote, server)
}

pub fn create_trace_logger() {
    let _ = tracing_subscriber::fmt()
        // enable everything
        .with_file(true)
        .with_line_number(true)
        .with_target(true)
        .with_thread_names(true)
        .with_span_events(FmtSpan::NONE)
        .with_ansi(false)
        .with_max_level(LevelFilter::DEBUG)
        // sets this to be the default, global collector for this application.
        .try_init();
}
