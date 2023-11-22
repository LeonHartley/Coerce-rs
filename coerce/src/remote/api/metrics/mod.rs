use crate::remote::api::Routes;
use crate::remote::system::{NodeId, RemoteActorSystem};

use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use metrics_exporter_prometheus::{BuildError, PrometheusBuilder, PrometheusHandle};
use metrics_util::MetricKindMask;

use std::time::Duration;

pub struct MetricsApi {
    recorder_handle: PrometheusHandle,
}

impl From<PrometheusHandle> for MetricsApi {
    fn from(recorder_handle: PrometheusHandle) -> Self {
        Self { recorder_handle }
    }
}

impl MetricsApi {
    pub fn create(
        node_id: NodeId,
        node_tag: &str,
        cluster_name: Option<&str>,
    ) -> Result<Self, BuildError> {
        let prometheus_builder = PrometheusBuilder::new();

        let mut builder = prometheus_builder.idle_timeout(
            MetricKindMask::COUNTER | MetricKindMask::HISTOGRAM,
            Some(Duration::from_secs(60 * 30)),
        );

        if let Some(cluster_name) = cluster_name {
            builder = builder.add_global_label("cluster", cluster_name.to_string());
        }

        builder = builder
            .add_global_label("node_id", format!("{}", node_id))
            .add_global_label("node_tag", node_tag);

        let recorder = builder.install_recorder()?;
        Ok(Self {
            recorder_handle: recorder,
        })
    }

    pub fn new(system: RemoteActorSystem) -> Result<MetricsApi, BuildError> {
        let node_id = system.node_id();
        let node_tag = system.node_tag();
        let cluster_name = system
            .config()
            .get_attributes()
            .get("cluster")
            .map(|s| s.as_ref());

        Self::create(node_id, node_tag, cluster_name)
    }
}

impl Routes for MetricsApi {
    fn routes(&self, router: Router) -> Router {
        router.route("/metrics", {
            let handle = self.recorder_handle.clone();

            get(|| async move {
                let handle = handle;
                handle.render().into_response()
            })
        })
    }
}
