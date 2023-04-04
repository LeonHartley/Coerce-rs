pub mod builder;
pub mod cluster;
pub mod system;

#[cfg(feature = "metrics")]
pub mod metrics;

pub mod openapi;

#[cfg(feature = "sharding")]
pub mod sharding;

use crate::actor::context::ActorContext;

use crate::actor::Actor;

use axum::Router;
use std::net::SocketAddr;
use tokio::sync::oneshot;
use tokio::sync::oneshot::Sender;

pub struct RemoteHttpApi {
    listen_addr: SocketAddr,
    routes: Option<Vec<Box<dyn Routes>>>,
    stop_tx: Option<Sender<()>>,
}

#[async_trait]
impl Actor for RemoteHttpApi {
    async fn started(&mut self, ctx: &mut ActorContext) {
        let node_id = ctx.system().remote().node_id();
        let listen_addr = self.listen_addr;

        let routes = self.routes.take().unwrap();

        let (stop_tx, stop_rx) = oneshot::channel();
        let _ = tokio::spawn(async move {
            info!("[node={}] http api listening on {}", node_id, &listen_addr);

            let mut app = Router::new();
            for route in routes {
                app = route.routes(app);
            }

            axum::Server::bind(&listen_addr)
                .serve(app.into_make_service())
                .with_graceful_shutdown(async { stop_rx.await.unwrap() })
                .await
                .unwrap()
        });

        self.stop_tx = Some(stop_tx);
    }

    async fn stopped(&mut self, _ctx: &mut ActorContext) {
        if let Some(stop_tx) = self.stop_tx.take() {
            let _ = stop_tx.send(());
        }
    }
}

impl RemoteHttpApi {
    pub fn new(listen_addr: SocketAddr, routes: Vec<Box<dyn Routes>>) -> Self {
        RemoteHttpApi {
            listen_addr,
            routes: Some(routes),
            stop_tx: None,
        }
    }
}

pub trait Routes: 'static + Send + Sync {
    fn routes(&self, router: Router) -> Router;
}
