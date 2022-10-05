use crate::config::KubernetesDiscoveryConfig;
use coerce::remote::system::RemoteActorSystem;
use k8s_openapi::api::core::v1::Pod;
use kube::api::ListParams;
use kube::{Api, Client, Config};
use std::sync::Arc;

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
                let hostname = pod_spec.hostname.unwrap();
                let subdomain = pod_spec.subdomain.unwrap();

                for container in pod_spec.containers {
                    let port = container
                        .ports
                        .unwrap()
                        .into_iter()
                        .filter(|p| p.name == config.coerce_remote_port_name)
                        .next()
                        .unwrap();

                    cluster_nodes.push(format!(
                        "{}.{}:{}",
                        hostname, subdomain, port.container_port
                    ));
                }
            }
        }

        debug!("discovered nodes: {:#?}", &cluster_nodes);
        Some(cluster_nodes)
    }
}
