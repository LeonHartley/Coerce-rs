use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message};
use crate::actor::{Actor, ActorRef};
use crate::persistent::journal::types::JournalTypes;
use crate::persistent::{PersistentActor, Recover};
use crate::remote::cluster::sharding::coordinator::allocation::AllocateShard;
use crate::remote::cluster::sharding::host::ShardHost;
use crate::remote::net::StreamReceiver;
use crate::remote::system::NodeId;
use crate::remote::RemoteActorRef;
use futures::StreamExt;
use std::collections::{HashMap, HashSet};

pub mod allocation;

pub type ShardId = u32;

struct ShardHostState {
    node_id: NodeId,
    node_tag: String,
    shards: HashSet<ShardId>,
    actor: ActorRef<ShardHost>,
}

pub struct ShardCoordinator {
    shard_entity: String,
    hosts: HashMap<NodeId, ShardHostState>,
    shards: HashMap<ShardId, NodeId>,
}

impl PersistentActor for ShardCoordinator {
    fn persistence_key(&self, _ctx: &ActorContext) -> String {
        format!("ShardCoordinator-{}", &self.shard_entity)
    }

    fn configure(types: &mut JournalTypes<Self>) {
        types.message::<AllocateShard>("AllocateShard");
    }
}
