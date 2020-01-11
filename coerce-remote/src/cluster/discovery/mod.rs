use futures::io::Error;
use std::io;
use std::net::Ipv4Addr;

pub mod dns;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DiscoveredWorker {
    host: Ipv4Addr,
    port: Option<i16>,
}

impl DiscoveredWorker {
    pub fn from_host(host: Ipv4Addr) -> DiscoveredWorker {
        DiscoveredWorker { host, port: None }
    }
}

#[async_trait]
pub trait ClusterSeed {
    async fn initial_workers(&self) -> Result<Vec<DiscoveredWorker>, ClusterSeedErr>;
}

#[derive(Debug)]
pub enum ClusterSeedErr {
    Io(io::Error),
}

impl From<io::Error> for ClusterSeedErr {
    fn from(e: Error) -> Self {
        ClusterSeedErr::Io(e)
    }
}
