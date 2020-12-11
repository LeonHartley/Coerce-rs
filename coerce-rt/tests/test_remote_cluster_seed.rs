use coerce_rt::remote::cluster::discovery::dns::DnsClusterSeed;
use coerce_rt::remote::cluster::discovery::{ClusterSeed, DiscoveredWorker};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::str::FromStr;

#[tokio::test]
pub async fn test_remote_cluster_seed_dns() {
    let seed_host = "one.one.one.one".to_owned();
    let dns_server = SocketAddr::from_str("1.1.1.1:53").unwrap();
    let mut cluster_seed = DnsClusterSeed::new(dns_server, seed_host).await;
    let discovered_workers = cluster_seed.initial_workers().await.unwrap();
    let expected_worker = DiscoveredWorker::from_host(IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)));

    assert!(discovered_workers.contains(&expected_worker));
}
