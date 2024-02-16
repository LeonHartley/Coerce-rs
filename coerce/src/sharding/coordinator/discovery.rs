use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message};
use crate::remote::cluster::node::RemoteNode;
use crate::sharding::coordinator::balancing::Rebalance;
use crate::sharding::coordinator::{ShardCoordinator, ShardHostState, ShardHostStatus};
use crate::sharding::host::ShardHost;

use crate::remote::stream::pubsub::Receive;
use crate::remote::stream::system::{ClusterEvent, SystemEvent, SystemTopic};
use crate::remote::system::NodeId;
use std::collections::hash_map::Entry;
use std::sync::Arc;

#[async_trait]
impl Handler<Receive<SystemTopic>> for ShardCoordinator {
    async fn handle(&mut self, message: Receive<SystemTopic>, ctx: &mut ActorContext) {
        match message.0.as_ref() {
            SystemEvent::Cluster(event) => match event {
                ClusterEvent::NodeAdded(node) => {
                    self.on_node_discovered(node.as_ref(), ctx);
                }

                ClusterEvent::NodeRemoved(node) => {
                    self.on_node_removed(node.id, ctx).await;
                }
                _ => {}
            },
            _ => {}
        }
    }
}

impl ShardCoordinator {
    pub fn on_node_discovered(&mut self, new_node: &RemoteNode, ctx: &ActorContext) {
        match self.hosts.entry(new_node.id) {
            Entry::Occupied(mut node) => {
                let node = node.get_mut();
                if node.status != ShardHostStatus::Ready {
                    node.status = ShardHostStatus::Ready;
                    self.schedule_full_rebalance(ctx);
                }
            }

            Entry::Vacant(vacant_entry) => {
                let remote = ctx.system().remote_owned();

                debug!(
                    "new shard host (node_id={}, tag={}, addr={})",
                    new_node.id, &new_node.tag, &new_node.addr
                );

                vacant_entry.insert(ShardHostState {
                    node_id: new_node.id,
                    node_tag: new_node.tag.clone(),
                    shards: Default::default(),
                    actor: ShardHost::remote_ref(&self.shard_entity, new_node.id, &remote),
                    status: ShardHostStatus::Ready/*TODO: shard hosts may not be immediately ready*/,
                });

                self.schedule_full_rebalance(ctx);
            }
        }
    }

    pub async fn on_node_removed(&mut self, node_id: NodeId, ctx: &mut ActorContext) {
        match self.hosts.entry(node_id) {
            Entry::Occupied(_) => self.handle(Rebalance::NodeUnavailable(node_id), ctx).await,
            Entry::Vacant(_) => {}
        }
    }
}
