use crate::actor::context::{ActorContext, ActorStatus};
use crate::actor::message::Handler;
use crate::actor::{Actor, ActorRefErr, IntoActor, LocalActorRef};
use crate::remote::cluster::sharding::coordinator::ShardCoordinator;
use crate::remote::cluster::sharding::host::ShardHost;
use crate::remote::stream::pubsub::{PubSub, StreamEvent, Subscription};
use crate::remote::stream::system::{ClusterEvent, SystemEvent, SystemTopic};
use crate::remote::system::NodeId;
use std::sync::Arc;

pub struct CoordinatorSpawner {
    node_id: NodeId,
    shard_entity: String,
    current_leader: Option<NodeId>,
    local_shard_host: LocalActorRef<ShardHost>,
    system_event_subscription: Option<Subscription>,
    coordinator: Option<LocalActorRef<ShardCoordinator>>,
}

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

                trace!(target: "ShardCoordinator-Spawner", "started shard coordinator for entity: {}", &self.shard_entity);
            }
            Err(e) => {
                error!(target: "ShardCoordinator-Spawner", "failed to spawn shard coordinator, e={}", e);
            }
        }
    }

    pub async fn stop_coordinator(&mut self) -> bool {
        if let Some(coordinator) = self.coordinator.take() {
            let result = coordinator.stop().await;
            match result {
                Ok(ActorStatus::Stopped) => true,
                Ok(_) => false,
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
        }

        self.system_event_subscription = Some(
            PubSub::subscribe::<Self, SystemTopic>(SystemTopic, ctx)
                .await
                .unwrap(),
        );
    }
}

#[async_trait]
impl Handler<StreamEvent<SystemTopic>> for CoordinatorSpawner {
    async fn handle(&mut self, message: StreamEvent<SystemTopic>, ctx: &mut ActorContext) {
        trace!(target: "ShardCoordinator-Spawner", "received system event");

        match message {
            StreamEvent::Receive(msg) => match msg.as_ref() {
                SystemEvent::Cluster(event) => match event {
                    &ClusterEvent::NodeAdded(node_id) => {}
                    &ClusterEvent::NodeRemoved(node_id) => {}
                    &ClusterEvent::LeaderChanged(leader_node_id) => {
                        if leader_node_id == self.node_id && self.coordinator.is_none() {
                            self.start_coordinator(ctx).await;
                        } else if self.stop_coordinator().await {
                            trace!(target: "ShardCoordinator-Spawner", "stopped coordinator");
                        }
                    }
                },
            },
            StreamEvent::Err => {}
        }
    }
}
