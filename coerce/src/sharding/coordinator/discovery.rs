use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message};
use crate::remote::cluster::node::RemoteNode;
use crate::sharding::coordinator::balancing::Rebalance;
use crate::sharding::coordinator::{ShardCoordinator, ShardHostState, ShardHostStatus};
use crate::sharding::host::ShardHost;

use crate::actor::IntoActorId;
use crate::remote::RemoteActorRef;
use std::collections::hash_map::Entry;
use std::sync::Arc;

pub struct NodeDiscovered(pub Arc<RemoteNode>);

pub struct NodeForgotten(pub Arc<RemoteNode>);

impl Message for NodeDiscovered {
    type Result = ();
}

impl Message for NodeForgotten {
    type Result = ();
}

#[async_trait]
impl Handler<NodeDiscovered> for ShardCoordinator {
    async fn handle(&mut self, message: NodeDiscovered, ctx: &mut ActorContext) {
        let new_node = message.0;
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
                    actor: RemoteActorRef::<ShardHost>::new(
                        format!("ShardHost-{}-{}", &self.shard_entity, new_node.id).into_actor_id(),
                        new_node.id,
                        remote,
                    )
                    .into(),
                    status: ShardHostStatus::Ready/*TODO: shard hosts may not be immediately ready*/,
                });

                self.schedule_full_rebalance(ctx);
            }
        }
    }
}

#[async_trait]
impl Handler<NodeForgotten> for ShardCoordinator {
    async fn handle(&mut self, message: NodeForgotten, ctx: &mut ActorContext) {
        match self.hosts.entry(message.0.id) {
            Entry::Occupied(_) => {
                self.handle(Rebalance::NodeUnavailable(message.0.id), ctx)
                    .await
            }
            Entry::Vacant(_) => {}
        }
    }
}
