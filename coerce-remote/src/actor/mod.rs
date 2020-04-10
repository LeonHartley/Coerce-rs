use crate::cluster::node::RemoteNodeStore;

use crate::context::RemoteActorContext;
use crate::handler::{
    ActorHandler, ActorMessageHandler, RemoteActorMarker, RemoteActorMessageMarker,
};
use crate::net::client::RemoteClientStream;
use coerce_rt::actor::context::ActorContext;
use coerce_rt::actor::message::Message;
use coerce_rt::actor::{Actor, ActorRef};
use std::any::TypeId;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

pub mod ext;
pub mod handler;
pub mod message;

pub struct RemoteClientRegistry {
    clients: HashMap<Uuid, Box<dyn RemoteClientStream + Sync + Send>>,
}

pub struct RemoteRegistry {
    nodes: RemoteNodeStore,
    context: Option<RemoteActorContext>,
}

pub(crate) type BoxedActorHandler = Box<dyn ActorHandler + Send + Sync>;

pub(crate) type BoxedMessageHandler = Box<dyn ActorMessageHandler + Send + Sync>;

pub struct RemoteHandlerTypes {
    actor_types: HashMap<TypeId, String>,
    handler_types: HashMap<TypeId, String>,
    message_handlers: HashMap<String, BoxedMessageHandler>,
    actor_handlers: HashMap<String, BoxedActorHandler>,
}

impl RemoteHandlerTypes {
    pub fn new(
        actor_types: HashMap<TypeId, String>,
        handler_types: HashMap<TypeId, String>,
        message_handlers: HashMap<String, BoxedMessageHandler>,
        actor_handlers: HashMap<String, BoxedActorHandler>,
    ) -> RemoteHandlerTypes {
        RemoteHandlerTypes {
            actor_types,
            handler_types,
            message_handlers,
            actor_handlers,
        }
    }

    pub fn handler_name<A: Actor, M: Message>(
        &self,
        marker: RemoteActorMessageMarker<A, M>,
    ) -> Option<String>
    where
        A: 'static + Send + Sync,
        M: 'static + Send + Sync,
        M::Result: 'static + Sync + Send,
    {
        self.handler_types
            .get(&marker.id())
            .map(|name| name.clone())
    }

    pub fn actor_name<A: Actor>(&self, marker: RemoteActorMarker<A>) -> Option<String>
    where
        A: 'static + Send + Sync,
    {
        self.actor_types.get(&marker.id()).map(|name| name.clone())
    }

    pub fn message_handler(&self, key: String) -> Option<BoxedMessageHandler> {
        self.message_handlers
            .get(&key)
            .map(|handler| handler.new_boxed())
    }

    pub fn actor_handler(&self, key: String) -> Option<BoxedActorHandler> {
        // self.actor_handlers
        //     .get(&key)
        //     .map(|handler| handler.new_boxed())
        None
    }
}

pub struct RemoteHandler {
    requests: HashMap<Uuid, RemoteRequest>,
}

pub struct RemoteRequest {
    pub res_tx: tokio::sync::oneshot::Sender<RemoteResponse>,
}

#[derive(Debug)]
pub enum RemoteResponse {
    Ok(Vec<u8>),
    Err(Vec<u8>),
    PingOk,
}

impl RemoteResponse {
    pub fn is_ok(&self) -> bool {
        match self {
            &RemoteResponse::Ok(..) | &RemoteResponse::PingOk => true,
            _ => false,
        }
    }

    pub fn is_err(&self) -> bool {
        match self {
            &RemoteResponse::Err(..) => true,
            _ => false,
        }
    }

    pub fn into_result(self) -> Result<Vec<u8>, Vec<u8>> {
        match self {
            RemoteResponse::Ok(buff) => Ok(buff),
            RemoteResponse::Err(buff) => Err(buff),
            _ => panic!("response is not a buffer"),
        }
    }
}

impl Actor for RemoteRegistry {}

impl Actor for RemoteHandler {}

impl Actor for RemoteClientRegistry {}

impl RemoteClientRegistry {
    pub async fn new(ctx: &mut ActorContext) -> ActorRef<RemoteClientRegistry> {
        ctx.new_anon_actor(RemoteClientRegistry {
            clients: HashMap::new(),
        })
        .await
        .expect("RemoteClientRegistry")
    }

    pub fn add_client<C: RemoteClientStream>(&mut self, node_id: Uuid, client: C)
    where
        C: 'static + Sync + Send,
    {
        self.clients.insert(node_id, Box::new(client));
    }
}

impl RemoteRegistry {
    pub async fn new(ctx: &mut ActorContext) -> ActorRef<RemoteRegistry> {
        ctx.new_anon_actor(RemoteRegistry {
            nodes: RemoteNodeStore::new(vec![]),
            context: None,
        })
        .await
        .expect("RemoteRegistry")
    }
}

impl RemoteHandler {
    pub async fn new(ctx: &mut ActorContext) -> ActorRef<RemoteHandler> {
        ctx.new_anon_actor(RemoteHandler {
            requests: HashMap::new(),
        })
        .await
        .expect("RemoteHandler")
    }
}
