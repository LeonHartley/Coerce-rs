pub struct KubernetesDiscoveryConfig {
    /// Pod label used to discover active Coerce cluster nodes
    pub pod_selection_label: Option<String>,

    /// Name of the port, as defined in the kubernetes pod spec
    pub coerce_remote_port_name: Option<String>,
}

impl Default for KubernetesDiscoveryConfig {
    fn default() -> Self {
        Self {
            pod_selection_label: Some("app=coerce".to_string()),
            coerce_remote_port_name: Some("coerce".to_string()),
        }
    }
}
