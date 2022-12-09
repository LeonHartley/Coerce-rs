pub enum Address {
    /// Uses the pod's IP address for coerce cluster communication
    PodIp,

    /// Uses the pod's hostname and subdomain name (if set)
    Hostname,
}

pub struct KubernetesDiscoveryConfig {
    /// Pod label used to discover active Coerce cluster nodes (environment variable: COERCE_K8S_POD_SELECTOR)
    pub pod_selection_label: Option<String>,

    /// Name of the port, as defined in the kubernetes pod spec (
    pub coerce_remote_port_name: Option<String>,

    pub cluster_node_address: Address,
}

impl Default for KubernetesDiscoveryConfig {
    fn default() -> Self {
        Self {
            pod_selection_label: Some(
                std::env::var("COERCE_K8S_POD_SELECTOR")
                    .map_or_else(|_e| "app=coerce".to_string(), |s| s),
            ),
            coerce_remote_port_name: std::env::var("COERCE_K8S_PORT_NAME")
                .map_or_else(|_e| Some("coerce".to_string()), Some),
            cluster_node_address: std::env::var("COERCE_K8S_ADDR_MODE").map_or_else(
                |_e| Address::PodIp,
                |s| {
                    if s.to_lowercase() == "hostname" {
                        Address::Hostname
                    } else {
                        Address::PodIp
                    }
                },
            ),
        }
    }
}
