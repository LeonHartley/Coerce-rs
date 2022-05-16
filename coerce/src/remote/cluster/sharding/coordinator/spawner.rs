use crate::actor::context::ActorContext;
use crate::actor::message::Handler;
use crate::actor::{Actor, IntoActor, LocalActorRef};
use crate::remote::cluster::sharding::coordinator::discovery::{NodeDiscovered, NodeForgotten};
use crate::remote::cluster::sharding::coordinator::ShardCoordinator;
use crate::remote::cluster::sharding::host::{LeaderAllocated, ShardHost};
use crate::remote::stream::pubsub::{PubSub, Receive, Subscription};
use crate::remote::stream::system::{ClusterEvent, SystemEvent, SystemTopic};
use crate::remote::system::NodeId;

pub struct CoordinatorSpawner {
    node_id: NodeId,
    shard_entity: String,
    current_leader: Option<NodeId>,
    local_shard_host: LocalActorRef<ShardHost>,
    system_event_subscription: Option<Subscription>,
    coordinator: Option<LocalActorRef<ShardCoordinator>>,
}

const COORDINATOR_SPAWNER: &str = "ShardCoordinator-Spawner";

impl CoordinatorSpawner {
    pub fn new(
        node_id: NodeId,
        shard_entity: String,
        local_shard_host: LocalActorRef<ShardHost>,
    ) -> CoordinatorSpawner {
        Self {
            node_id,
            shard_entity,
            local_shard_host,
            current_leader: None,
            system_event_subscription: None,
            coordinator: None,
        }
    }

    pub async fn start_coordinator(&mut self, ctx: &mut ActorContext) {
        let coordinator =
            ShardCoordinator::new(self.shard_entity.clone(), self.local_shard_host.clone())
                .into_actor(
                    Some(format!("ShardCoordinator-{}", &self.shard_entity)),
                    &ctx.system(),
                )
                .await;
        match coordinator {
            Ok(coordinator) => {
                self.coordinator = Some(coordinator);

                debug!(
                    target: COORDINATOR_SPAWNER,
                    "[node={}] started shard coordinator for entity type={}",
                    self.node_id,
                    &self.shard_entity
                );
            }
            Err(e) => {
                error!(
                    target: COORDINATOR_SPAWNER,
                    "[node={}] failed to spawn shard coordinator, e={}", self.node_id, e
                );
            }
        }
    }

    pub async fn stop_coordinator(&mut self) -> bool {
        if let Some(coordinator) = self.coordinator.take() {
            let result = coordinator.stop().await;
            match result {
                Ok(_) => true,
                Err(_) => true,
            }
        } else {
            false
        }
    }
}

#[async_trait]
impl Actor for CoordinatorSpawner {
    async fn started(&mut self, ctx: &mut ActorContext) {
        let remote = ctx.system().remote();
        self.current_leader = remote.current_leader();
        if let Some(current_leader) = self.current_leader {
            if current_leader == remote.node_id() {
                self.start_coordinator(ctx).await;
            }
        } else {
            debug!(
                target: COORDINATOR_SPAWNER,
                "[node={}] no leader allocated", self.node_id
            );
        }

        self.system_event_subscription = Some(
            PubSub::subscribe::<Self, SystemTopic>(SystemTopic, ctx)
                .await
                .unwrap(),
        );
    }
}

#[async_trait]
impl Handler<Receive<SystemTopic>> for CoordinatorSpawner {
    async fn handle(&mut self, message: Receive<SystemTopic>, ctx: &mut ActorContext) {
        debug!(
            target: COORDINATOR_SPAWNER,
            "[node={}] received system event - {:?}", self.node_id, &message.0
        );

        match message.0.as_ref() {
            SystemEvent::Cluster(event) => match event {
                ClusterEvent::NodeAdded(node) => {
                    if let Some(coordinator) = &self.coordinator {
                        let _ = coordinator.notify(NodeDiscovered(node.clone()));
                    }
                }

                ClusterEvent::NodeRemoved(node) => {
                    if let Some(coordinator) = &self.coordinator {
                        let _ = coordinator.notify(NodeForgotten(node.clone()));
                    }
                }

                ClusterEvent::LeaderChanged(leader_node_id) => {
                    let leader_node_id = *leader_node_id;
                    debug!(
                        target: COORDINATOR_SPAWNER,
                        "[node={}] Leader changed, leader_node_id={}", self.node_id, leader_node_id,
                    );

                    if leader_node_id == self.node_id && self.coordinator.is_none() {
                        self.start_coordinator(ctx).await;
                    } else if self.stop_coordinator().await {
                        trace!(
                            target: COORDINATOR_SPAWNER,
                            "[node={}] stopped coordinator",
                            self.node_id
                        );
                    }

                    self.local_shard_host.notify(LeaderAllocated);
                }
            },
        }
    }
}
