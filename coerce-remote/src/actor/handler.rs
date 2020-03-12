use crate::actor::message::{
    ClientWrite, GetHandler, GetNodes, HandlerName, PopRequest, PushRequest, RegisterClient,
    RegisterNode, RegisterNodes, SetContext,
};
use crate::actor::{
    BoxedHandler, RemoteClientRegistry, RemoteHandler, RemoteRegistry, RemoteRequest,
};
use crate::cluster::node::RemoteNode;
use crate::codec::json::JsonCodec;
use crate::context::RemoteActorContext;
use crate::net::client::{RemoteClient, RemoteClientStream};
use coerce_rt::actor::context::ActorHandlerContext;
use coerce_rt::actor::message::{Handler, Message};
use coerce_rt::actor::Actor;

use std::collections::HashMap;

use uuid::Uuid;

#[async_trait]
impl Handler<SetContext> for RemoteRegistry {
    async fn handle(&mut self, message: SetContext, _ctx: &mut ActorHandlerContext) {
        self.context = Some(message.0);
    }
}

#[async_trait]
impl Handler<GetNodes> for RemoteRegistry {
    async fn handle(
        &mut self,
        _message: GetNodes,
        _ctx: &mut ActorHandlerContext,
    ) -> Vec<RemoteNode> {
        self.nodes.get_all()
    }
}

#[async_trait]
impl Handler<GetHandler> for RemoteHandler {
    async fn handle(
        &mut self,
        message: GetHandler,
        _ctx: &mut ActorHandlerContext,
    ) -> Option<BoxedHandler> {
        self.handlers
            .get(&message.0)
            .map(|handler| handler.new_boxed())
    }
}

#[async_trait]
impl Handler<PushRequest> for RemoteHandler {
    async fn handle(&mut self, message: PushRequest, _ctx: &mut ActorHandlerContext) {
        self.requests.insert(message.0, message.1);
    }
}

#[async_trait]
impl Handler<PopRequest> for RemoteHandler {
    async fn handle(
        &mut self,
        message: PopRequest,
        _ctx: &mut ActorHandlerContext,
    ) -> Option<RemoteRequest> {
        self.requests.remove(&message.0)
    }
}

#[async_trait]
impl<A: Actor, M: Message> Handler<HandlerName<A, M>> for RemoteHandler
where
    A: 'static + Send + Sync,
    M: 'static + Send + Sync,
    M::Result: 'static + Sync + Send,
{
    async fn handle(
        &mut self,
        message: HandlerName<A, M>,
        _ctx: &mut ActorHandlerContext,
    ) -> Option<String> {
        self.handler_types
            .get(&message.marker.id())
            .map(|name| name.clone())
    }
}

#[async_trait]
impl<T: RemoteClientStream> Handler<RegisterClient<T>> for RemoteClientRegistry
where
    T: 'static + Sync + Send,
{
    async fn handle(&mut self, message: RegisterClient<T>, _ctx: &mut ActorHandlerContext) {
        self.add_client(message.0, message.1);

        trace!(target: "RemoteRegistry", "client {} registered", message.0);
    }
}

#[async_trait]
impl Handler<RegisterNodes> for RemoteRegistry {
    async fn handle(&mut self, message: RegisterNodes, _ctx: &mut ActorHandlerContext) {
        let mut remote_ctx = self.context.as_ref().unwrap().clone();
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
    async fn handle(&mut self, message: RegisterNode, _ctx: &mut ActorHandlerContext) {
        self.nodes.add(message.0);
    }
}

#[async_trait]
impl Handler<ClientWrite> for RemoteClientRegistry {
    async fn handle(&mut self, message: ClientWrite, _ctx: &mut ActorHandlerContext) {
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
    ctx: &RemoteActorContext,
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