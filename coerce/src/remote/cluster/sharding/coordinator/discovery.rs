use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message};
use crate::remote::cluster::node::RemoteNode;
use crate::remote::cluster::sharding::coordinator::balancing::Rebalance;
use crate::remote::cluster::sharding::coordinator::{
    ShardCoordinator, ShardHostState, ShardHostStatus,
};
use crate::remote::cluster::sharding::host::ShardHost;
use crate::remote::system::NodeId;
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
            Entry::Occupied(_) => {}
            Entry::Vacant(vacant_entry) => {
                let remote = ctx.system().remote_owned();

                debug!(target: "ShardCoordinator", "new shard host (node_id={}, tag={}, addr={})", new_node.id, &new_node.tag, &new_node.addr);
                vacant_entry.insert(ShardHostState {
                    node_id: new_node.id,
                    node_tag: new_node.tag.clone(),
                    shards: Default::default(),
                    actor: RemoteActorRef::<ShardHost>::new(
                        format!("ShardHost-{}-{}", &self.shard_entity, new_node.id),
                        new_node.id,
                        remote,
                    )
                    .into(),
                    status: ShardHostStatus::Ready,
                });
            }
        }
    }
}

#[async_trait]
impl Handler<NodeForgotten> for ShardCoordinator {
    async fn handle(&mut self, message: NodeForgotten, ctx: &mut ActorContext) {
        match self.hosts.entry(message.0.id) {
            Entry::Occupied(_) => self.handle(Rebalance::Node(message.0.id), ctx).await,
            Entry::Vacant(_) => {}
        }
    }
}
