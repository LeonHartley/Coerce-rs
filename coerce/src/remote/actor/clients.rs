use crate::actor::context::ActorContext;
use crate::actor::message::Handler;
use crate::actor::scheduler::ActorType;
use crate::actor::system::ActorSystem;
use crate::actor::{Actor, LocalActorRef};
use crate::remote::actor::message::{ClientConnected, ClientWrite, NewClient, RemoveClient};
use crate::remote::net::client::send::Write;
use crate::remote::net::client::RemoteClient;
use crate::remote::system::NodeId;
use std::collections::hash_map::Entry;
use std::collections::HashMap;

pub struct RemoteClientRegistry {
    node_addr_registry: HashMap<String, LocalActorRef<RemoteClient>>,
    node_id_registry: HashMap<NodeId, LocalActorRef<RemoteClient>>,
}

#[async_trait]
impl Actor for RemoteClientRegistry {
    async fn stopped(&mut self, _ctx: &mut ActorContext) {
        for client in &self.node_id_registry {
            let _ = client.1.stop().await;
        }
    }
}

impl RemoteClientRegistry {
    pub async fn new(ctx: &ActorSystem) -> LocalActorRef<RemoteClientRegistry> {
        ctx.new_actor(
            "remote-client-registry",
            RemoteClientRegistry {
                node_addr_registry: HashMap::new(),
                node_id_registry: HashMap::new(),
            },
            ActorType::Tracked,
        )
        .await
        .expect("RemoteClientRegistry")
    }
}

#[async_trait]
impl Handler<NewClient> for RemoteClientRegistry {
    async fn handle(
        &mut self,
        message: NewClient,
        _ctx: &mut ActorContext,
    ) -> Option<LocalActorRef<RemoteClient>> {
        let node_addr_entry = self.node_addr_registry.entry(message.addr.clone());
        let entry = match node_addr_entry {
            Entry::Vacant(vacant_entry) => vacant_entry,
            Entry::Occupied(entry) => {
                return {
                    debug!("RemoteClient already exists for addr={}", &message.addr);
                    let client = entry.get();
                    Some(client.clone())
                }
            }
        };

        debug!("creating RemoteClient, addr={}", &message.addr);

        let client_actor =
            RemoteClient::new(message.addr.clone(), message.system, message.client_type).await;

        debug!("created RemoteClient, addr={}", &message.addr);
        entry.insert(client_actor.clone());

        Some(client_actor)
    }
}

#[async_trait]
impl Handler<RemoveClient> for RemoteClientRegistry {
    async fn handle(&mut self, message: RemoveClient, _: &mut ActorContext) {
        if let Some(node_id) = message.node_id {
            self.node_id_registry.remove(&node_id);
        }

        self.node_addr_registry.remove(&message.addr);

        debug!(
            addr = &message.addr,
            node_id = &message.node_id,
            "client removed"
        )
    }
}

#[async_trait]
impl Handler<ClientConnected> for RemoteClientRegistry {
    async fn handle(&mut self, message: ClientConnected, _ctx: &mut ActorContext) {
        self.node_id_registry
            .insert(message.remote_node_id, message.client_actor_ref);
    }
}

#[async_trait]
impl Handler<ClientWrite> for RemoteClientRegistry {
    async fn handle(&mut self, message: ClientWrite, _ctx: &mut ActorContext) {
        let node_id = message.0;
        let message = message.1;

        // TODO: we could open multiple clients per node and use some routing mechanism
        //       to potentially improve throughput, whilst still maintaining message ordering

        if let Some(client) = self.node_id_registry.get(&node_id) {
            trace!("emitting message ({:?}) to node_id={}", &message, &node_id);
            client.notify(Write(message)).expect("send client msg");
            trace!("written data to client");
        } else {
            // TODO: should we buffer the message incase the client will eventually exist
            warn!("attempted to write message to node_id={} but no client was registered (message={:?})", &node_id, &message);
        }
    }
}
