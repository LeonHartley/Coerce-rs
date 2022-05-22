use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message};
use crate::actor::Actor;
use crate::remote::actor::message::SetRemote;
use crate::remote::cluster::node::{NodeIdentity, NodeStatus, RemoteNode};
use crate::remote::net::client::RemoteClientRef;
use crate::remote::stream::pubsub::PubSub;
use crate::remote::stream::system::{ClusterEvent, SystemEvent, SystemTopic};
use crate::remote::system::{NodeId, RemoteActorSystem};
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::oneshot::Sender;
use uuid::Uuid;

#[derive(Default)]
pub struct NodeDiscovery {
    discovering_nodes: HashSet<NodeId>,
    discovered_nodes_by_addr: HashMap<String, Arc<NodeIdentity>>,
    discovered_nodes_by_id: HashMap<NodeId, Arc<NodeIdentity>>,
    remote_system: Option<RemoteActorSystem>,
}

impl Actor for NodeDiscovery {}

pub struct Discover {
    pub seed: Seed,
    pub on_discovery_complete: Option<Sender<()>>,
}

pub enum Seed {
    Addr(String),
    Nodes(Vec<RemoteNode>),
}

pub struct Forget(pub String);

impl Message for Discover {
    type Result = ();
}

impl Message for Forget {
    type Result = ();
}

#[async_trait]
impl Handler<SetRemote> for NodeDiscovery {
    async fn handle(&mut self, message: SetRemote, _ctx: &mut ActorContext) {
        self.remote_system = Some(message.0);
    }
}

#[async_trait]
impl Handler<Discover> for NodeDiscovery {
    async fn handle(&mut self, message: Discover, ctx: &mut ActorContext) {
        let remote = self.remote_system.clone().unwrap();

        match message.seed {
            Seed::Addr(addr) => {
                if self.discovered_nodes_by_addr.contains_key(&addr) {
                    info!("node (addr={}) already discovered", &addr);

                    if let Some(on_discovery_complete) = message.on_discovery_complete {
                        let _ = on_discovery_complete.send(());
                    }

                    return;
                }

                if let Some(seed_node) = self.get_node_identity(addr.clone(), &remote).await {
                    let mut discovered_nodes: HashMap<NodeId, Arc<NodeIdentity>> = HashMap::new();
                    discovered_nodes.insert(seed_node.node.id, seed_node.clone());

                    self.discover_nodes(&remote, seed_node, &mut discovered_nodes)
                        .await;

                    if discovered_nodes.len() > 1 {
                        let nodes_to_discover: Vec<Arc<NodeIdentity>> =
                            discovered_nodes.values().cloned().collect();
                        for node_identity in nodes_to_discover {
                            self.discover_nodes(&remote, node_identity, &mut discovered_nodes)
                                .await;
                        }
                    } else if discovered_nodes.len() == 0 {
                        if let Some(discovery_complete) = message.on_discovery_complete {
                            let _ = discovery_complete.send(());
                        }

                        return;
                    }

                    let _ = self.actor_ref(ctx).notify(Discover {
                        seed: Seed::Nodes(
                            discovered_nodes.values().map(|n| n.node.clone()).collect(),
                        ),
                        on_discovery_complete: message.on_discovery_complete,
                    });
                } else {
                    warn!(
                        "[node={}] unable to identify node (addr={})",
                        remote.node_id(),
                        &addr
                    );

                    if let Some(discovery_complete) = message.on_discovery_complete {
                        let _ = discovery_complete.send(());
                    }
                }
            }

            Seed::Nodes(nodes) => {
                let current_nodes: HashSet<NodeId> = remote
                    .get_nodes()
                    .await
                    .into_iter()
                    .filter(|n| n.status != NodeStatus::Terminated)
                    .map(|n| n.id)
                    .collect();
                let node_count = nodes.len();

                info!("discovering {} nodes", node_count);

                for node in nodes {
                    if !current_nodes.contains(&node.id)
                        && !self.discovering_nodes.contains(&node.id)
                    {
                        let node_addr = node.addr.clone();
                        if let Some(client) = remote.get_remote_client(node_addr).await {
                            remote.register_node(node.clone()).await;

                            let seed_nodes = remote
                                .get_nodes()
                                .await
                                .into_iter()
                                .filter(|n| n.status != NodeStatus::Terminated)
                                .map(|n| n.into())
                                .collect();

                            tokio::spawn(discover_node_handshake(
                                node,
                                remote.clone(),
                                client,
                                seed_nodes,
                            ));
                        }
                    }
                }

                info!("discovered {} nodes", node_count);

                if let Some(discovery_complete) = message.on_discovery_complete {
                    let _ = discovery_complete.send(());
                }
            }
        }
    }
}

#[async_trait]
impl Handler<Forget> for NodeDiscovery {
    async fn handle(&mut self, message: Forget, _ctx: &mut ActorContext) {
        if let Some(identity) = self.discovered_nodes_by_addr.remove(&message.0) {
            debug!(
                "forgetting node (addr={}, id={})",
                &identity.node.addr, identity.node.id
            );

            let node = self.discovered_nodes_by_id.remove(&identity.node.id);
            if let Some(node) = node {
                if let Some(system) = self.remote_system.as_ref() {
                    let system = system.clone();
                    let node = Arc::new(node.node.clone());

                    let _ = tokio::spawn(async move {
                        PubSub::publish_locally(
                            SystemTopic,
                            SystemEvent::Cluster(ClusterEvent::NodeRemoved(node)),
                            &system,
                        )
                        .await;
                    });
                }
            }
        }
    }
}

struct NodeDiscovered {
    node: RemoteNode,
    successful: bool,
}

impl Message for NodeDiscovered {
    type Result = ();
}

#[async_trait]
impl Handler<NodeDiscovered> for NodeDiscovery {
    async fn handle(&mut self, message: NodeDiscovered, _ctx: &mut ActorContext) {
        let remote = self.remote_system.as_ref().unwrap();
        if message.successful {
            if self.discovering_nodes.remove(&message.node.id) {
                PubSub::publish_locally(
                    SystemTopic,
                    SystemEvent::Cluster(ClusterEvent::NodeAdded(Arc::new(message.node))),
                    remote,
                )
                .await;
            }
        } else {
            // retry handshake etc?
            warn!(
                "handshake to node (addr={}, id={}) failed (from node_id={}) failed",
                message.node.addr,
                message.node.id,
                remote.node_id()
            );
        }
    }
}

async fn discover_node_handshake(
    node: RemoteNode,
    system: RemoteActorSystem,
    client: RemoteClientRef,
    seed_nodes: Vec<RemoteNode>,
) {
    let request_id = Uuid::new_v4();
    let handshake_result = client.handshake(request_id, seed_nodes).await;
    let successful = handshake_result.is_ok();
    match handshake_result {
        Ok(_) => {
            info!(
                    "successfully discovered peer (addr={}, id={}, tag={}, started_at={:?}, request_id={})",
                    &node.addr, &node.id, &node.tag, &node.node_started_at, &request_id,
                );

            PubSub::publish_locally(
                SystemTopic,
                SystemEvent::Cluster(ClusterEvent::NodeAdded(Arc::new(node.clone()))),
                &system,
            )
            .await;
        }
        Err(e) => {
            error!("error while attempting to handshake with node (from node={}) - {} (addr={}, id={}, tag={}, started_at={:?}), request_id={}",
                   system.node_id(), e, node.addr, node.id, node.tag, node.node_started_at, request_id,
                );
        }
    }

    let _ = system
        .node_discovery()
        .notify(NodeDiscovered { node, successful });
}

impl NodeDiscovery {
    async fn discover_nodes(
        &mut self,
        remote: &RemoteActorSystem,
        seed_node: Arc<NodeIdentity>,
        discovered_nodes: &mut HashMap<NodeId, Arc<NodeIdentity>>,
    ) {
        for node in &seed_node.peers {
            if node.id == remote.node_id() {
                continue;
            }

            // TODO: validation

            match discovered_nodes.entry(node.id) {
                Entry::Vacant(entry) => {
                    let node_identity = self.get_node_identity(node.addr.clone(), remote).await;
                    if let Some(node_identity) = node_identity {
                        entry.insert(node_identity);
                    }
                }
                _ => continue,
            }
        }
    }

    pub async fn get_node_identity(
        &mut self,
        addr: String,
        remote: &RemoteActorSystem,
    ) -> Option<Arc<NodeIdentity>> {
        if let Some(discovered_node) = self.discovered_nodes_by_addr.get(&addr) {
            return Some(discovered_node.clone());
        }

        let client = remote.get_remote_client(addr.clone()).await;
        if let Some(client) = client {
            let identity = client.identify().await;
            if let Ok(Some(identity)) = identity {
                let identity = Arc::new(identity);
                let addr = identity.node.addr.clone();
                let node_id = identity.node.id;

                self.discovered_nodes_by_addr.insert(addr, identity.clone());
                self.discovered_nodes_by_id
                    .insert(node_id, identity.clone());

                Some(identity)
            } else {
                info!("unable to identify node");
                None
            }
        } else {
            warn!(
                "no client created for addr={}, unable to identify node",
                &addr
            );

            None
        }
    }
}
