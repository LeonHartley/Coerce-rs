use crate::actor::system::ActorSystem;
use crate::actor::{IntoActor, LocalActorRef};
use crate::remote::api::{RemoteHttpApi, Routes};
use axum::Router;
use std::net::SocketAddr;

pub struct HttpApiBuilder {
    listen_addr: Option<SocketAddr>,
    routes: Vec<Box<dyn Routes>>,
}

impl HttpApiBuilder {
    pub fn new() -> Self {
        Self {
            listen_addr: None,
            routes: vec![],
        }
    }

    pub fn listen_addr(mut self, listen_addr: SocketAddr) -> Self {
        self.listen_addr = Some(listen_addr);
        self
    }

    pub fn routes(mut self, route: impl Routes) -> Self {
        self.routes.push(Box::new(route));
        self
    }

    pub async fn start(self, system: &ActorSystem) -> LocalActorRef<RemoteHttpApi> {
        let listen_addr = self
            .listen_addr
            .expect("listen_addr is required, no default is defined yet (TODO)");

        RemoteHttpApi::new(listen_addr, self.routes)
            .into_actor(Some("RemoteHttpApi"), system)
            .await
            .unwrap()
    }
}
