use crate::config::{Address, KubernetesDiscoveryConfig};
use coerce::remote::system::RemoteActorSystem;
use k8s_openapi::api::core::v1::Pod;
use kube::api::ListParams;
use kube::{Api, Client};

#[macro_use]
extern crate tracing;
pub mod config;

pub struct KubernetesDiscovery {
    system: RemoteActorSystem,
}

impl KubernetesDiscovery {
    pub async fn discover(config: KubernetesDiscoveryConfig) -> Option<Vec<String>> {
        let client = Client::try_default()
            .await
            .expect("failed to initialise k8s client");
        let api = Api::<Pod>::default_namespaced(client);

        let params = ListParams {
            label_selector: config.pod_selection_label.clone(),
            ..Default::default()
        };

        let pods = api.list(&params).await;
        let mut cluster_nodes = vec![];

        if let Ok(pods) = pods {
            debug!("pods={:#?}", &pods.items);

            let pods = pods.items;
            for pod in pods {
                let pod_spec = pod.spec.unwrap();
                let pod_status = pod.status.unwrap();

                debug!("pod_status={:?}", pod_status);

                // TODO: Check that the pod is available before using it as a
                //       seed node.

                let addr = match &config.cluster_node_address {
                    Address::PodIp => {
                        let pod_ip = pod_status.pod_ip;
                        if pod_ip.is_none() {
                            continue;
                        }

                        pod_ip.unwrap()
                    }

                    Address::Hostname => {
                        let hostname = pod_spec.hostname.unwrap();
                        let subdomain = pod_spec.subdomain.unwrap();

                        format!("{hostname}.{subdomain}")
                    }
                };

                for container in pod_spec.containers {
                    let port = container
                        .ports
                        .unwrap()
                        .into_iter()
                        .find(|p| p.name == config.coerce_remote_port_name);
                    if let Some(port) = port {
                        cluster_nodes.push(format!("{}:{}", addr, port.container_port));
                    }
                }
            }
        }

        debug!("discovered nodes: {:#?}", &cluster_nodes);
        Some(cluster_nodes)
    }
}
