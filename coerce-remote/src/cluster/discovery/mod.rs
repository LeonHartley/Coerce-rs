use futures::io::Error;
use std::io;
use std::net::{IpAddr};

pub mod dns;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DiscoveredWorker {
    host: IpAddr,
    port: Option<i16>,
}

impl DiscoveredWorker {
    pub fn from_host(host: IpAddr) -> DiscoveredWorker {
        DiscoveredWorker { host, port: None }
    }
}

#[async_trait]
pub trait ClusterSeed {
    async fn initial_workers(&mut self) -> Result<Vec<DiscoveredWorker>, ClusterSeedErr>;
}

#[derive(Debug)]
pub enum ClusterSeedErr {
    Io(io::Error),
    Err(String),
}

impl From<io::Error> for ClusterSeedErr {
    fn from(e: Error) -> Self {
        ClusterSeedErr::Io(e)
    }
}
