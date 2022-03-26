pub mod cluster;
pub mod metrics;
pub mod sharding;

use crate::actor::message::{Handler, Message};
use crate::actor::{Actor, LocalActorRef};
use crate::remote::system::RemoteActorSystem;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::ops::Deref;

pub struct RemoteHttpApi {
    pub system: RemoteActorSystem,
    pub listen_addr: SocketAddr,
    router: Router,
}

impl RemoteHttpApi {
    pub fn new(listen_addr: SocketAddr, system: RemoteActorSystem) -> Self {
        RemoteHttpApi {
            system,
            listen_addr,
            router: Router::new(),
        }
    }

    pub fn routes<R>(mut self, mut route: &R) -> Self
    where
        R: Routes,
    {
        self.router = route.routes(self.router);
        self
    }

    pub async fn start(self) {
        let app = self.router.route("/version", get(|| async { VERSION }));

        info!(
            "[node={}] http api listening on {}",
            &self.system.node_id(),
            &self.listen_addr
        );
        axum::Server::bind(&self.listen_addr)
            .serve(app.into_make_service())
            .await
            .unwrap();
    }
}

pub trait Routes {
    fn routes(&self, router: Router) -> Router;
}

const VERSION: &str = env!("CARGO_PKG_VERSION");

// basic handler that responds with a static string
async fn version() -> &'static str {
    VERSION
}
