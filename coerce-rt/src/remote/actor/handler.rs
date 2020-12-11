use crate::actor::context::ActorContext;
use crate::actor::message::Handler;
use crate::remote::actor::message::{
    ClientWrite, GetNodes, PopRequest, PushRequest, RegisterClient, RegisterNode, RegisterNodes,
    SetSystem,
};
use crate::remote::actor::{RemoteClientRegistry, RemoteHandler, RemoteRegistry, RemoteRequest};
use crate::remote::cluster::node::RemoteNode;
use crate::remote::codec::json::JsonCodec;
use crate::remote::system::RemoteActorSystem;
use crate::remote::net::client::{RemoteClient, RemoteClientStream};

use std::collections::HashMap;

use uuid::Uuid;

#[async_trait]
impl Handler<SetSystem> for RemoteRegistry {
    async fn handle(&mut self, message: SetSystem, _ctx: &mut ActorContext) {
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

        trace!("unregistered nodes {:?}", &unregistered_nodes);
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

async fn connect_all(
    nodes: Vec<RemoteNode>,
    current_nodes: Vec<RemoteNode>,
    ctx: &RemoteActorSystem,
) -> HashMap<Uuid, Option<RemoteClient<JsonCodec>>> {
    let mut clients = HashMap::new();
    for node in nodes {
        let addr = node.addr.to_string();
        match RemoteClient::connect(
            addr,
            ctx.clone(),
            JsonCodec::new(),
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
