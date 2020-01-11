use crate::cluster::discovery::{ClusterSeed, ClusterSeedErr, DiscoveredWorker};
use dnsclient::r#async::DNSClient;
use dnsclient::UpstreamServer;
use std::io;
use std::net::{Ipv4Addr, SocketAddr};

pub struct DnsClusterSeed {
    dns_client: DNSClient,
    seed_host: String,
}

impl DnsClusterSeed {
    pub fn new(servers: Vec<SocketAddr>, seed_host: String) -> DnsClusterSeed {
        let dns_client = DNSClient::new(servers.into_iter().map(UpstreamServer::new).collect());

        DnsClusterSeed {
            dns_client,
            seed_host,
        }
    }

    pub async fn all_a_records(&self, host: &str) -> Result<Vec<Ipv4Addr>, io::Error> {
        self.dns_client.query_a(host).await
    }
}

#[async_trait]
impl ClusterSeed for DnsClusterSeed {
    async fn initial_workers(&self) -> Result<Vec<DiscoveredWorker>, ClusterSeedErr> {
        let records = self.all_a_records(self.seed_host.as_str()).await?;

        Ok(records
            .into_iter()
            .map(DiscoveredWorker::from_host)
            .collect())
    }
}
