use crate::remote::cluster::node::RemoteNodeStore;

use crate::actor::message::Message;
use crate::actor::system::ActorSystem;
use crate::actor::{scheduler::ActorType, Actor, ActorId, LocalActorRef};
use crate::remote::handler::{
    ActorHandler, ActorMessageHandler, RemoteActorMarker, RemoteActorMessageMarker,
};
use crate::remote::net::client::RemoteClientStream;
use crate::remote::system::RemoteActorSystem;
use std::any::TypeId;
use std::collections::HashMap;

use crate::actor::scheduler::ActorType::{Anonymous, Tracked};
use crate::remote::stream::pubsub::Subscription;

use uuid::Uuid;

pub mod handler;
pub mod heartbeat;
pub mod message;

pub struct RemoteClientRegistry {
    clients: HashMap<Uuid, Box<dyn RemoteClientStream + Sync + Send>>,
}

pub struct RemoteRegistry {
    nodes: RemoteNodeStore,
    actors: HashMap<ActorId, Uuid>,
    system: Option<RemoteActorSystem>,
    system_event_subscription: Option<Subscription>,
}

pub(crate) type BoxedActorHandler = Box<dyn ActorHandler + Send + Sync>;

pub(crate) type BoxedMessageHandler = Box<dyn ActorMessageHandler + Send + Sync>;

pub struct RemoteHandlerTypes {
    node_tag: String,
    actor_types: HashMap<TypeId, String>,
    handler_types: HashMap<TypeId, String>,
    message_handlers: HashMap<String, BoxedMessageHandler>,
    actor_handlers: HashMap<String, BoxedActorHandler>,
}

impl RemoteHandlerTypes {
    pub fn new(
        node_tag: String,
        actor_types: HashMap<TypeId, String>,
        handler_types: HashMap<TypeId, String>,
        message_handlers: HashMap<String, BoxedMessageHandler>,
        actor_handlers: HashMap<String, BoxedActorHandler>,
    ) -> RemoteHandlerTypes {
        RemoteHandlerTypes {
            node_tag,
            actor_types,
            handler_types,
            message_handlers,
            actor_handlers,
        }
    }

    pub fn node_tag(&self) -> &str {
        &self.node_tag
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

    pub fn message_handler(&self, key: &String) -> Option<BoxedMessageHandler> {
        self.message_handlers
            .get(key)
            .map(|handler| handler.new_boxed())
    }

    pub fn actor_handler(&self, key: &String) -> Option<BoxedActorHandler> {
        self.actor_handlers
            .get(key)
            .map(|handler| handler.new_boxed())
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
    pub async fn new(ctx: &ActorSystem, system_tag: &str) -> LocalActorRef<RemoteClientRegistry> {
        ctx.new_actor(
            format!("RemoteClientRegistry-{}", system_tag),
            RemoteClientRegistry {
                clients: HashMap::new(),
            },
            ActorType::Anonymous,
        )
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
    pub async fn new(ctx: &ActorSystem, system_tag: &str) -> LocalActorRef<RemoteRegistry> {
        ctx.new_actor(
            format!("RemoteRegistry-{}", &system_tag),
            RemoteRegistry {
                actors: HashMap::new(),
                nodes: RemoteNodeStore::new(vec![]),
                system: None,
                system_event_subscription: None,
            },
            Anonymous,
        )
        .await
        .expect("RemoteRegistry")
    }
}

impl RemoteHandler {
    pub async fn new(ctx: &ActorSystem, system_tag: &str) -> LocalActorRef<RemoteHandler> {
        ctx.new_actor(
            format!("RemoteHandler-{}", system_tag),
            RemoteHandler {
                requests: HashMap::new(),
            },
            Anonymous,
        )
        .await
        .expect("RemoteHandler")
    }
}
