use crate::remote::cluster::discovery::{ClusterSeed, ClusterSeedErr, DiscoveredWorker};

use std::net::{IpAddr, SocketAddr};

use std::str::FromStr;
use trust_dns_client::client::{AsyncClient, ClientHandle};

use trust_dns_client::rr::{DNSClass, Name, RecordType};
use trust_dns_client::udp::UdpClientStream;
use trust_dns_proto::udp::UdpResponse;

pub struct DnsClusterSeed {
    client: AsyncClient<UdpResponse>,
    seed_host: String,
}

impl DnsClusterSeed {
    pub async fn new(upstream: SocketAddr, seed_host: String) -> DnsClusterSeed {
        let stream = UdpClientStream::<tokio::net::UdpSocket>::new(upstream);
        let (client, bg) = AsyncClient::connect(stream).await.unwrap();

        tokio::spawn(bg);

        DnsClusterSeed { client, seed_host }
    }

    pub async fn all_a_records(&mut self, host: &str) -> Result<Vec<IpAddr>, ClusterSeedErr> {
        match self
            .client
            .query(Name::from_str(host).unwrap(), DNSClass::IN, RecordType::A)
            .await
        {
            Ok(res) => Ok(res
                .answers()
                .into_iter()
                .filter_map(|a| a.rdata().to_ip_addr())
                .collect()),

            Err(e) => Err(ClusterSeedErr::Err(format!("{:?}", e))),
        }
    }
}

#[async_trait]
impl ClusterSeed for DnsClusterSeed {
    async fn initial_workers(&mut self) -> Result<Vec<DiscoveredWorker>, ClusterSeedErr> {
        let seed_host = self.seed_host.clone();
        let records = self.all_a_records(&seed_host).await?;

        Ok(records
            .into_iter()
            .map(DiscoveredWorker::from_host)
            .collect())
    }
}
