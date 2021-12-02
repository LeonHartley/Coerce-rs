use crate::actor::context::ActorContext;

use crate::actor::ActorRef;
use crate::persistent::journal::types::JournalTypes;
use crate::persistent::{PersistentActor, Recover, RecoverSnapshot};
use crate::remote::cluster::sharding::coordinator::allocation::AllocateShard;
use crate::remote::cluster::sharding::host::ShardHost;

use crate::remote::system::NodeId;

use crate::actor::message::Message;
use crate::persistent::journal::snapshot::Snapshot;
use crate::persistent::journal::PersistErr;
use std::collections::{HashMap, HashSet};

pub mod allocation;

pub type ShardId = u32;

pub struct ShardHostState {
    pub node_id: NodeId,
    pub node_tag: String,
    pub shards: HashSet<ShardId>,
    pub actor: ActorRef<ShardHost>,
}

pub struct ShardCoordinator {
    shard_entity: String,
    hosts: HashMap<NodeId, ShardHostState>,
    shards: HashMap<ShardId, NodeId>,
}

#[async_trait]
impl PersistentActor for ShardCoordinator {
    fn persistence_key(&self, _ctx: &ActorContext) -> String {
        format!("ShardCoordinator-{}", &self.shard_entity)
    }

    fn configure(types: &mut JournalTypes<Self>) {
        types.message::<AllocateShard>("AllocateShard");
    }

    async fn pre_recovery(&mut self, ctx: &mut ActorContext) {
        let hosts = ctx.system().remote().get_nodes().await;
        info!("got potential shard hosts, {:?}", hosts);
    }
}

impl ShardCoordinator {
    pub fn new(shard_entity: String) -> ShardCoordinator {
        ShardCoordinator {
            shard_entity,
            hosts: Default::default(),
            shards: Default::default(),
        }
    }

    pub fn add_host(&mut self, host: ShardHostState) {
        self.hosts.insert(host.node_id, host);
    }
}
