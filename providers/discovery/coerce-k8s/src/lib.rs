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
    pub async fn discover(
        system: RemoteActorSystem,
        config: KubernetesDiscoveryConfig,
    ) -> Option<Vec<String>> {
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
            let pods = pods.items;
            for pod in pods {
                let pod_ip = pod.status.unwrap().pod_ip;
                let pod_spec = pod.spec.unwrap();

                if let Some(pod_ip) = pod_ip {
                    let coerce_remote_port = pod_spec
                        .containers
                        .into_iter()
                        .flat_map(|c| c.ports)
                        .map(|p| {
                            p.into_iter()
                                .filter(|p| {
                                    p.name.as_ref() == config.coerce_remote_port_name.as_ref()
                                })
                                .next()
                        })
                        .next();

                    if let Some(Some(port)) = coerce_remote_port {
                        cluster_nodes.push(format!("{}:{}", pod_ip, port.container_port))
                    }
                }
            }
        }

        info!("discovered nodes: {:#?}", &cluster_nodes);
        Some(cluster_nodes)
    }
}
//
// pub trait Test {
//   fn test(&self) -> bool;
// }
//
// impl Test for Remote
