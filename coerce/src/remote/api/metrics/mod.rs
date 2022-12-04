use crate::actor::{Actor, LocalActorRef};
use crate::remote::api::sharding::ShardingApi;
use crate::remote::api::Routes;
use crate::remote::system::RemoteActorSystem;
use axum::extract::Path;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use metrics_exporter_prometheus::{
    BuildError, PrometheusBuilder, PrometheusHandle, PrometheusRecorder,
};
use metrics_util::MetricKindMask;
use std::net::SocketAddr;
use std::time::Duration;

pub struct MetricsApi {
    recorder_handle: PrometheusHandle,
}

impl MetricsApi {
    pub fn new(system: RemoteActorSystem) -> Result<MetricsApi, BuildError> {
        let prometheus_builder = PrometheusBuilder::new();

        let recorder = prometheus_builder
            .idle_timeout(
                MetricKindMask::COUNTER | MetricKindMask::HISTOGRAM,
                Some(Duration::from_secs(60 * 30)),
            )
            .add_global_label("node_id", system.node_id().to_string())
            .install_recorder()?;

        Ok(Self {
            recorder_handle: recorder,
        })
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
