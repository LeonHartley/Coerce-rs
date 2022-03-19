use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message};
use crate::actor::scheduler::timer::{Timer, TimerTick};
use crate::actor::{Actor, IntoActor, LocalActorRef};
use crate::remote::cluster::node::NodeStatus::Healthy;
use crate::remote::cluster::node::{NodeStatus, RemoteNodeState};
use crate::remote::net::message::SessionEvent;
use crate::remote::net::proto::network::{Ping, Pong};
use crate::remote::stream::pubsub::PubSub;
use crate::remote::stream::system::ClusterEvent::LeaderChanged;
use crate::remote::stream::system::{ClusterEvent, SystemEvent, SystemTopic};
use crate::remote::system::{NodeId, NodeRpcErr, RemoteActorSystem};
use chrono::{DateTime, Utc, MIN_DATETIME};
use futures::future::Map;
use futures::{future::join_all, FutureExt, TryFutureExt};
use std::cmp::Ordering;
use std::future::Future;
use std::ops::Add;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::time::error::Elapsed;
use tokio::time::Timeout;
use uuid::Uuid;

pub struct Heartbeat {
    config: Arc<HeartbeatConfig>,
    system: RemoteActorSystem,
    heartbeat_timer: Option<Timer>,
    last_heartbeat: Option<DateTime<Utc>>,
}

pub struct HeartbeatConfig {
    pub interval: Duration,
    pub ping_timeout: Duration,
    pub unhealthy_node_heartbeat_timeout: Duration,
    pub terminated_node_heartbeat_timeout: Duration,
}

impl Default for HeartbeatConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_millis(500),
            ping_timeout: Duration::from_secs(15),
            unhealthy_node_heartbeat_timeout: Duration::from_millis(500),
            terminated_node_heartbeat_timeout: Duration::from_secs(30),
        }
    }
}

impl Heartbeat {
    pub async fn start(
        config: HeartbeatConfig,
        remote_system: &RemoteActorSystem,
    ) -> LocalActorRef<Heartbeat> {
        let system = remote_system.clone();
        let config = Arc::new(config);
        Heartbeat {
            config,
            system,
            heartbeat_timer: None,
            last_heartbeat: None,
        }
        .into_actor(Some(format!("heartbeat-{}", &remote_system.node_tag())), &remote_system.actor_system())
        .await
        .expect("heartbeat actor")
    }
}

#[derive(Clone)]
struct HeartbeatTick;

impl Message for HeartbeatTick {
    type Result = ();
}

impl TimerTick for HeartbeatTick {}

#[async_trait]
impl Actor for Heartbeat {
    async fn started(&mut self, ctx: &mut ActorContext) {
        trace!(
            "starting heartbeat timer (tick duration={} millis), node_id={}",
            &self.config.interval.as_millis(),
            self.system.node_id()
        );

        self.heartbeat_timer = Some(Timer::start(
            self.actor_ref(ctx),
            self.config.interval,
            HeartbeatTick,
        ));
    }
}

#[async_trait]
impl Handler<HeartbeatTick> for Heartbeat {
    async fn handle(&mut self, _msg: HeartbeatTick, _ctx: &mut ActorContext) {
        let node_tag = self.system.node_tag();
        let current_node = self.system.node_id();

        let now = Instant::now();
        trace!(target: "Heartbeat", "heartbeat tick, node_id={}, node_tag={}", &current_node, &node_tag);

        let nodes = self.system.get_nodes().await;
        let mut ping_futures = vec![];
        let mut updates = vec![];

        for node in nodes {
            if node.id == current_node {
                let mut node = node;
                node.status = NodeStatus::Healthy;
                updates.push(node);

                continue;
            }

            ping_futures.push(ping_node(node, self.system.clone(), self.config.clone()))
        }

        updates.extend(join_all(ping_futures).await);

        trace!(
            "current_node = {}, nodes: {:?}, heartbeat took {} ms",
            current_node,
            &updates,
            now.elapsed().as_millis()
        );

        updates.sort_by(|a, b| {
            match Ord::cmp(
                &a.node_started_at.unwrap_or(MIN_DATETIME),
                &b.node_started_at.unwrap_or(MIN_DATETIME),
            ) {
                Ordering::Equal => Ord::cmp(&a.id, &b.id),
                ordering => ordering,
            }
        });

        let oldest_node = updates.first();
        if let Some(oldest_node) = oldest_node {
            if Some(oldest_node.id) != self.system.current_leader() {
                self.system.update_leader(oldest_node.id);
                info!(
                    "[node={}] leader of cluster: {:?}, current_node_tag={}",
                    self.system.node_id(), oldest_node, &node_tag
                );

                let id = oldest_node.id;
                let system = self.system.clone();
                tokio::spawn(async move {
                    let system = system;
                    let _ = PubSub::publish_locally(
                        SystemTopic,
                        SystemEvent::Cluster(LeaderChanged(id)),
                        &system,
                    )
                    .await;
                });
            }
        }

        self.system.update_nodes(updates).await;
        self.last_heartbeat = Some(Utc::now())
    }
}

pub enum PingResult {
    Ok(Pong),
    Timeout,
    Err(NodeRpcErr),
}

impl PingResult {
    pub fn is_ok(&self) -> bool {
        match &self {
            PingResult::Ok(_) => true,
            _ => false,
        }
    }
}

fn ping_rpc(
    node: RemoteNodeState,
    system: &RemoteActorSystem,
    timeout: Duration,
) -> impl Future<Output = (RemoteNodeState, PingResult)> + '_ {
    let message_id = Uuid::new_v4();
    let event = SessionEvent::Ping(Ping {
        message_id: message_id.to_string(),
        ..Ping::default()
    });

    tokio::time::timeout(
        timeout,
        system.node_rpc_proto::<Pong>(message_id, event, node.id),
    )
    .map(move |result| {
        (
            node,
            match result {
                Ok(res) => match res {
                    Ok(pong) => PingResult::Ok(pong),
                    Err(e) => PingResult::Err(e),
                },
                Err(_e) => PingResult::Timeout,
            },
        )
    })
}

async fn ping_node(
    node: RemoteNodeState,
    system: RemoteActorSystem,
    heartbeat_config: Arc<HeartbeatConfig>,
) -> RemoteNodeState {
    let start = Instant::now();
    let (mut node, ping) = ping_rpc(node, &system, heartbeat_config.ping_timeout).await;
    let ping_latency = start.elapsed();

    if ping.is_ok() {
        node.last_heartbeat = Some(Utc::now());
        node.ping_latency = Some(ping_latency);
    }

    node.status = node_status(
        system.node_id(),
        node.id,
        node.status,
        &node.last_heartbeat,
        ping,
        ping_latency,
        &heartbeat_config,
    );

    node
}

fn node_status(
    node_id: NodeId,
    peer_node_id: NodeId,
    previous_status: NodeStatus,
    last_heartbeat: &Option<DateTime<Utc>>,
    ping: PingResult,
    ping_latency: Duration,
    config: &HeartbeatConfig,
) -> NodeStatus {
    match ping {
        PingResult::Ok(_) => {
            if ping_latency > config.unhealthy_node_heartbeat_timeout {
                warn!("[node={}] node_id={} took {}ms to respond to ping, marking as unhealthy", node_id, peer_node_id,
                    ping_latency.as_millis());
                NodeStatus::Unhealthy
            } else {
                if previous_status != NodeStatus::Healthy {
                    info!( "[node={}] remote node_id={} is now healthy", node_id, peer_node_id);
                }

                NodeStatus::Healthy
            }
        }

        PingResult::Timeout => {
            let terminated = last_heartbeat.map_or(true, |h| {
                h.add(chrono::Duration::from_std(config.terminated_node_heartbeat_timeout).unwrap())
                    >= Utc::now()
            });

            if terminated {
                error!(target: "Heartbeat", "node_id={} has never responded to ping or it has been longer than {} ms since last successful heartbeat, marking as terminated", node_id,
                    config.terminated_node_heartbeat_timeout.as_millis());
                NodeStatus::Terminated
            } else {
                warn!(target: "Heartbeat", "node_id={} did not respond to ping within {} ms, marking as unhealthy", node_id, config.ping_timeout.as_millis());
                NodeStatus::Unhealthy
            }
        }

        PingResult::Err(e) => {
            if previous_status == NodeStatus::Unhealthy {
                error!(
                    target: "Heartbeat",
                    "error during ping rpc to node={}, error={}, marking node as terminated",
                    node_id, e
                );
                NodeStatus::Terminated
            } else {
                error!(
                    target: "Heartbeat",
                    "error during ping rpc to node={}, error={}, marking node as unhealthy",
                    node_id, e
                );
                NodeStatus::Unhealthy
            }
        }
    }
}
