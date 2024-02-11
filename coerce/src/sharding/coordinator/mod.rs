use crate::actor::context::ActorContext;

use crate::actor::{Actor, ActorRef, LocalActorRef, ScheduledNotify};
use crate::persistent::journal::types::JournalTypes;
use crate::persistent::PersistentActor;
use crate::sharding::coordinator::allocation::AllocateShard;
use crate::sharding::host::ShardHost;

use crate::remote::system::NodeId;

use crate::actor::message::Handler;
use crate::remote::cluster::node::NodeStatus::{Healthy, Joining};
use crate::remote::heartbeat::Heartbeat;
use crate::remote::stream::pubsub::{PubSub, Subscription};
use crate::remote::stream::system::SystemTopic;
use crate::sharding::coordinator::balancing::Rebalance;
use std::collections::{HashMap, HashSet};
use std::time::Duration;

pub mod allocation;
pub mod balancing;
pub mod discovery;
pub mod factory;
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

#[derive(Eq, PartialEq, Debug, Copy, Clone, Serialize, Deserialize)]
pub enum ShardHostStatus {
    Unknown,
    Starting,
    Ready,
    Unavailable,
}

impl ShardHostStatus {
    pub fn is_available(&self) -> bool {
        matches!(&self, ShardHostStatus::Ready)
    }
}

pub struct ShardCoordinator {
    shard_entity: String,
    local_shard_host: LocalActorRef<ShardHost>,
    hosts: HashMap<NodeId, ShardHostState>,
    shards: HashMap<ShardId, NodeId>,
    reallocating_shards: HashSet<ShardId>,
    scheduled_rebalance: Option<ScheduledRebalance>,
    self_node_id: Option<NodeId>,
    system_event_subscription: Option<Subscription>,
}

type ScheduledRebalance = ScheduledNotify<ShardCoordinator, Rebalance>;

#[async_trait]
impl PersistentActor for ShardCoordinator {
    fn persistence_key(&self, _ctx: &ActorContext) -> String {
        format!("{}-ShardCoordinator", &self.shard_entity)
    }

    fn configure(types: &mut JournalTypes<Self>) {
        types.message::<AllocateShard>("AllocateShard");
    }

    async fn pre_recovery(&mut self, ctx: &mut ActorContext) {
        let remote = ctx.system().remote();
        Heartbeat::register(ctx.boxed_actor_ref(), remote);

        let node_id = remote.node_id();
        let node_tag = remote.node_tag().to_string();

        self.self_node_id = Some(node_id);
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
            shard_entity = &self.shard_entity,
            "shard coordinator started",
        );

        self.system_event_subscription = Some(
            PubSub::subscribe::<Self, SystemTopic>(SystemTopic, ctx)
                .await
                .unwrap(),
        );

        let potential_hosts = remote.get_nodes().await;
        for host in potential_hosts {
            if host.id != node_id {
                self.add_host(ShardHostState {
                    node_id: host.id,
                    node_tag: String::default(),
                    shards: HashSet::new(),
                    actor: ShardHost::remote_ref(&self.shard_entity, host.id, remote),
                    status: if host.status == Healthy || host.status == Joining {
                        ShardHostStatus::Ready
                    } else {
                        ShardHostStatus::Unavailable
                    },
                });
            }
        }
    }

    async fn stopped(&mut self, ctx: &mut ActorContext) {
        Heartbeat::remove(ctx.id(), ctx.system().remote());
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
            scheduled_rebalance: None,
            self_node_id: None,
            system_event_subscription: None,
        }
    }

    pub fn schedule_full_rebalance(&mut self, ctx: &ActorContext) {
        if let Some(scheduled_rebalance) = self.scheduled_rebalance.take() {
            scheduled_rebalance.cancel();
        }

        self.scheduled_rebalance = Some(
            self.actor_ref(ctx)
                .scheduled_notify(Rebalance::All, Duration::from_millis(500)),
        );
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
