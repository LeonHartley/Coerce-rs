use crate::actor::context::ActorContext;

use crate::actor::{ActorRef, LocalActorRef};
use crate::persistent::journal::types::JournalTypes;
use crate::persistent::{PersistentActor, Recover, RecoverSnapshot};
use crate::remote::cluster::sharding::coordinator::allocation::AllocateShard;
use crate::remote::cluster::sharding::host::ShardHost;

use crate::remote::system::NodeId;

use crate::actor::message::Message;
use crate::persistent::journal::snapshot::Snapshot;
use crate::persistent::journal::PersistErr;
use crate::remote::cluster::node::RemoteNode;
use crate::remote::RemoteActorRef;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

pub mod allocation;
pub mod balancing;
pub mod discovery;
pub mod spawner;
pub mod stats;
pub mod stream;

pub type ShardId = u32;

#[derive(Debug)]
pub struct ShardHostState {
    pub node_id: NodeId,
    pub node_tag: String,
    pub shards: HashSet<ShardId>,
    pub actor: ActorRef<ShardHost>,
    pub status: ShardHostStatus,
}

#[derive(Debug)]
pub enum ShardHostStatus {
    Starting,
    Ready,
    Unavailable,
}

pub struct ShardCoordinator {
    shard_entity: String,
    local_shard_host: LocalActorRef<ShardHost>,
    hosts: HashMap<NodeId, ShardHostState>,
    shards: HashMap<ShardId, NodeId>,
    reallocating_shards: HashSet<ShardId>,
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
        let remote = ctx.system().remote();
        let node_id = remote.node_id();
        let node_tag = remote.node_tag().to_string();

        self.add_host(ShardHostState {
            node_id,
            node_tag,
            shards: Default::default(),
            actor: self.local_shard_host.clone().into(),
            status: ShardHostStatus::Ready,
        });

        // TODO: start a healthcheck actor/timer checking all allocated shards ensuring they're up,
        //       or rebalance/rehydrate if necessary

        info!(
            "shard coordinator started (shard_entity={})",
            &self.shard_entity
        );

        let potential_hosts = remote.get_nodes().await;
        for host in potential_hosts {
            if host.id != node_id {
                self.add_host(ShardHostState {
                    node_id: host.id,
                    node_tag: String::default(),
                    shards: HashSet::new(),
                    actor: RemoteActorRef::<ShardHost>::new(
                        format!("ShardHost-{}-{}", &self.shard_entity, host.id),
                        host.id,
                        remote.clone(),
                    )
                    .into(),
                    status: ShardHostStatus::Ready/*TODO:starting..ready upon host acknowledgement*/,
                });
            }
        }
    }
}

impl ShardCoordinator {
    pub fn new(
        shard_entity: String,
        local_shard_host: LocalActorRef<ShardHost>,
    ) -> ShardCoordinator {
        ShardCoordinator {
            shard_entity,
            local_shard_host,
            hosts: Default::default(),
            shards: Default::default(),
            reallocating_shards: Default::default(),
        }
    }

    pub fn add_host(&mut self, host: ShardHostState) {
        self.hosts.insert(host.node_id, host);
    }
}

impl ShardHostState {
    pub fn is_ready(&self) -> bool {
        match &self.status {
            ShardHostStatus::Ready => true,
            _ => false,
        }
    }
}
