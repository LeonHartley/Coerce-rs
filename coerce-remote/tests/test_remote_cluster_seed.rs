use coerce_remote::cluster::discovery::dns::DnsClusterSeed;
use coerce_remote::cluster::discovery::{ClusterSeed, DiscoveredWorker};
use std::net::{SocketAddr, Ipv4Addr};
use std::str::FromStr;

#[tokio::test]
pub async fn test_remote_cluster_seed_dns() {
    let seed_host = "one.one.one.one".to_owned();
    let dns_servers = vec![SocketAddr::from_str("1.1.1.1:53").unwrap()];
    let cluster_seed = DnsClusterSeed::new(dns_servers, seed_host);

    let discovered_workers = cluster_seed.initial_workers().await.unwrap();
    let expected_worker = DiscoveredWorker::from_host(Ipv4Addr::new(1, 1, 1, 1));

    assert!(discovered_workers.contains(&expected_worker));
}
