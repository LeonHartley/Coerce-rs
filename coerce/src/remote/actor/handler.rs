use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message};
use crate::remote::actor::message::{
    ClientWrite, GetActorNode, GetNodes, PopRequest, PushRequest, RegisterActor, RegisterClient,
    RegisterNode, RegisterNodes, SetRemote,
};
use crate::remote::actor::{
    RemoteClientRegistry, RemoteHandler, RemoteRegistry, RemoteRequest, RemoteResponse,
};
use crate::remote::cluster::node::RemoteNode;
use crate::remote::codec::json::JsonCodec;
use crate::remote::net::client::{RemoteClient, RemoteClientStream};
use crate::remote::system::RemoteActorSystem;

use std::collections::HashMap;

use crate::remote::net::message::SessionEvent;
use crate::remote::net::proto::protocol::ActorAddress;
use protobuf::parse_from_bytes;
use std::str::FromStr;
use tokio::sync::oneshot::error::RecvError;
use uuid::Uuid;

#[async_trait]
impl Handler<SetRemote> for RemoteRegistry {
    async fn handle(&mut self, message: SetRemote, _ctx: &mut ActorContext) {
        self.system = Some(message.0);
    }
}

#[async_trait]
impl Handler<GetNodes> for RemoteRegistry {
    async fn handle(&mut self, _message: GetNodes, _ctx: &mut ActorContext) -> Vec<RemoteNode> {
        self.nodes.get_all()
    }
}

#[async_trait]
impl Handler<PushRequest> for RemoteHandler {
    async fn handle(&mut self, message: PushRequest, _ctx: &mut ActorContext) {
        self.requests.insert(message.0, message.1);
    }
}

#[async_trait]
impl Handler<PopRequest> for RemoteHandler {
    async fn handle(
        &mut self,
        message: PopRequest,
        _ctx: &mut ActorContext,
    ) -> Option<RemoteRequest> {
        self.requests.remove(&message.0)
    }
}

#[async_trait]
impl<T: RemoteClientStream> Handler<RegisterClient<T>> for RemoteClientRegistry
where
    T: 'static + Sync + Send,
{
    async fn handle(&mut self, message: RegisterClient<T>, _ctx: &mut ActorContext) {
        self.add_client(message.0, message.1);

        trace!(target: "RemoteRegistry", "client {} registered", message.0);
    }
}

#[async_trait]
impl Handler<RegisterNodes> for RemoteRegistry {
    async fn handle(&mut self, message: RegisterNodes, _ctx: &mut ActorContext) {
        let mut remote_ctx = self.system.as_ref().unwrap().clone();
        let nodes = message.0;

        let unregistered_nodes = nodes
            .iter()
            .filter(|node| node.id != remote_ctx.node_id() && !self.nodes.is_registered(node.id))
            .map(|node| node.clone())
            .collect();

        trace!("registering new nodes {:?}", &unregistered_nodes);
        let current_nodes = self.nodes.get_all();

        let clients = connect_all(unregistered_nodes, current_nodes, &remote_ctx).await;
        for (_node_id, client) in clients {
            if let Some(client) = client {
                remote_ctx.register_client(client.node_id, client).await;
            }
        }

        for node in nodes {
            self.nodes.add(node)
        }
    }
}

#[async_trait]
impl Handler<RegisterNode> for RemoteRegistry {
    async fn handle(&mut self, message: RegisterNode, _ctx: &mut ActorContext) {
        self.nodes.add(message.0);
    }
}

#[async_trait]
impl Handler<ClientWrite> for RemoteClientRegistry {
    async fn handle(&mut self, message: ClientWrite, _ctx: &mut ActorContext) {
        let client_id = message.0;
        let message = message.1;

        if let Some(client) = self.clients.get_mut(&client_id) {
            client.send(message).await.expect("send client msg");
            trace!(target: "RemoteRegistry", "writing data to client")
        } else {
            trace!(target: "RemoteRegistry", "client {} not found", &client_id);
        }
    }
}

#[async_trait]
impl Handler<GetActorNode> for RemoteRegistry {
    async fn handle(&mut self, message: GetActorNode, _: &mut ActorContext) {
        let id = message.actor_id;
        let current_system = self.system.as_ref().unwrap().node_id();
        let assigned_registry_node = self.nodes.get_by_key(&id).map(|n| n.id);

        let assigned_registry_node = assigned_registry_node.map_or_else(
            || {
                trace!("no nodes configured, assigning locally");
                current_system
            },
            |n| n,
        );

        trace!("{:?}", &self.nodes.get_all());

        if &assigned_registry_node == &current_system {
            trace!(target: "RemoteRegistry::GetActorNode", "searching locally, {}", current_system);
            let node = self.actors.get(&id).map(|s| *s);

            trace!(target: "RemoteRegistry::GetActorNode", "found: {:?}", &node);
            message.sender.send(node);
        } else {
            let system = self.system.as_ref().unwrap().clone();
            let sender = message.sender;

            trace!(target: "RemoteRegistry::GetActorNode", "asking remotely, current_sys={}, target_sys={}", current_system, assigned_registry_node);
            tokio::spawn(async move {
                let message_id = Uuid::new_v4();
                let mut system = system;
                let (res_tx, res_rx) = tokio::sync::oneshot::channel();

                trace!(target: "RemoteRegistry::GetActorNode", "remote request={}", message_id);
                system.push_request(message_id, res_tx).await;

                trace!(target: "RemoteRegistry::GetActorNode", "sending actor lookup request to={}", assigned_registry_node);
                system
                    .send_message(
                        assigned_registry_node,
                        SessionEvent::FindActor(message_id, id),
                    )
                    .await;

                trace!(target: "RemoteRegistry::GetActorNode", "lookup sent, waiting for result");
                match res_rx.await {
                    Ok(RemoteResponse::Ok(res)) => {
                        let res = parse_from_bytes::<ActorAddress>(&res);
                        match res {
                            Ok(res) => {
                                sender.send(if res.get_node_id().is_empty() {
                                    None
                                } else {
                                    Some(Uuid::from_str(res.get_node_id()).unwrap())
                                });
                            }
                            Err(e) => {
                                panic!("failed to decode message - {}", e.to_string());
                            }
                        }
                    }
                    _ => panic!("get actornode failed"),
                }
            });
        }
    }
}

#[async_trait]
impl Handler<RegisterActor> for RemoteRegistry {
    async fn handle(&mut self, message: RegisterActor, ctx: &mut ActorContext) {
        trace!(target: "RemoteRegistry::RegisterActor", "Registering actor: {:?}", &message);

        match message.node_id {
            Some(node_id) => {
                trace!("registering actor locally {}", node_id);
                self.actors.insert(message.actor_id, node_id);
            }

            None => {
                if let Some(system) = self.system.as_mut() {
                    let node_id = system.node_id();
                    let id = message.actor_id;

                    let assigned_registry_node =
                        self.nodes.get_by_key(&id).map_or_else(|| node_id, |n| n.id);

                    if &assigned_registry_node == &node_id {
                        trace!("registering actor locally {}", assigned_registry_node);
                        self.actors.insert(id, node_id);
                    } else {
                        let event = SessionEvent::RegisterActor(ActorAddress { node_id: node_id.to_string(), actor_id: id, ..ActorAddress::default() });
                        system.send_message(assigned_registry_node, event).await;
                    }
                }
            }
        }
    }
}

async fn connect_all(
    nodes: Vec<RemoteNode>,
    current_nodes: Vec<RemoteNode>,
    ctx: &RemoteActorSystem,
) -> HashMap<Uuid, Option<RemoteClient>> {
    let mut clients = HashMap::new();
    for node in nodes {
        let addr = node.addr.to_string();
        match RemoteClient::connect(
            addr,
            ctx.clone(),
            Some(current_nodes.clone()),
        )
        .await
        {
            Ok(client) => {
                trace!(target: "RemoteRegistry", "connected to node");
                clients.insert(node.id, Some(client));
            }
            Err(_) => {
                clients.insert(node.id, None);
                warn!(target: "RemoteRegistry", "failed to connect to discovered worker");
            }
        }
    }

    clients
}
