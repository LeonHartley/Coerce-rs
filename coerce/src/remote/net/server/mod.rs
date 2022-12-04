use crate::actor::scheduler::ActorType::Anonymous;
use crate::actor::LocalActorRef;
use crate::remote::net::server::session::store::{NewSession, RemoteSessionStore};
use crate::remote::net::server::session::RemoteSession;
use crate::remote::system::RemoteActorSystem;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

pub mod session;

pub struct RemoteServer {
    cancellation_token: CancellationToken,
}

#[derive(Debug)]
pub enum RemoteServerErr {
    Startup,
    StreamErr(tokio::io::Error),
}

pub type RemoteServerConfigRef = Arc<RemoteServerConfig>;

#[derive(Debug)]
pub struct RemoteServerConfig {
    /// The address to listen for Coerce cluster client connections
    pub listen_addr: String,

    /// The address advertised by this node via the handshake
    pub external_node_addr: String,

    /// When true, incoming node addresses will be overwritten with the IP address
    /// used by the inbound client, rather than the address provided by
    /// the node via the handshake.
    pub override_incoming_node_addr: bool,

    /// The guard used for incoming connections
    pub connection_guard: ConnectionGuard,
}

#[derive(Debug)]
pub enum ConnectionGuard {
    /// All connections will be allowed, no token validation will take place.
    None,

    /// Connections will be verified by checking the incoming join token, can be validated using the configured token.
    Jwt { secret: Vec<u8> },
}

impl Default for ConnectionGuard {
    fn default() -> ConnectionGuard {
        Self::None
    }
}

impl RemoteServerConfig {
    pub fn new(
        listen_addr: String,
        external_node_addr: String,
        override_incoming_node_addr: bool,
    ) -> Self {
        Self {
            listen_addr,
            external_node_addr,
            override_incoming_node_addr,
            connection_guard: ConnectionGuard::default(),
        }
    }
}

impl RemoteServer {
    pub fn new() -> Self {
        RemoteServer {
            cancellation_token: CancellationToken::new(),
        }
    }

    pub async fn start(
        &mut self,
        config: RemoteServerConfig,
        system: RemoteActorSystem,
    ) -> Result<(), tokio::io::Error> {
        debug!(
            "starting remote server (node_id={}), config: {:#?}",
            system.node_id(),
            &config
        );

        let listener = tokio::net::TcpListener::bind(&config.listen_addr).await?;

        let session_store = system
            .actor_system()
            .new_actor(
                format!("RemoteSessionStore-{}", system.node_tag()),
                RemoteSessionStore::new(),
                Anonymous,
            )
            .await
            .unwrap();

        let remote_server_config = Arc::new(config);
        tokio::spawn(server_loop(
            listener,
            session_store,
            self.cancellation_token.clone(),
            remote_server_config,
        ));

        Ok(())
    }

    pub fn stop(&self) {
        self.cancellation_token.cancel();
    }
}

pub async fn cancellation(cancellation_token: CancellationToken) {
    cancellation_token.cancelled().await
}

pub async fn accept(
    listener: &tokio::net::TcpListener,
    cancellation_token: CancellationToken,
) -> Option<tokio::io::Result<(tokio::net::TcpStream, SocketAddr)>> {
    tokio::select! {
        _ = cancellation(cancellation_token) => {
            None
        }

        res = listener.accept() => {
            Some(res)
        }
    }
}

pub async fn server_loop(
    listener: tokio::net::TcpListener,
    session_store: LocalActorRef<RemoteSessionStore>,
    cancellation_token: CancellationToken,
    remote_server_config: RemoteServerConfigRef,
) {
    loop {
        match accept(&listener, cancellation_token.clone()).await {
            Some(Ok((stream, addr))) => {
                let remote_server_config = remote_server_config.clone();
                trace!("client accepted {}", addr);
                let session_id = uuid::Uuid::new_v4();

                // TODO: Before creating a session, we need to validate the received token.

                let session = session_store
                    .send(NewSession(RemoteSession::new(
                        session_id,
                        addr,
                        stream,
                        remote_server_config,
                    )))
                    .await;

                if let Err(e) = session {
                    error!(
                        "error creating session actor (session_id={}, addr={}), error: {:?}",
                        session_id, addr, e
                    );
                }
            }
            Some(Err(e)) => error!("error accepting client: {:?}", e),
            None => break,
        }
    }

    info!("tcp listener {:?} stopped", &listener)
}
