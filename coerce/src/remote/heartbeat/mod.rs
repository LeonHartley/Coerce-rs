use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message};
use crate::actor::scheduler::timer::{Timer, TimerTick};
use crate::actor::system::ActorSystem;
use crate::actor::{Actor, IntoActor, LocalActorRef};
use crate::remote::actor::message::SetRemote;

use crate::remote::cluster::node::{NodeStatus, RemoteNodeState};

use crate::remote::net::proto::network::Pong;
use crate::remote::stream::pubsub::PubSub;
use crate::remote::stream::system::ClusterEvent::LeaderChanged;
use crate::remote::stream::system::{SystemEvent, SystemTopic};
use crate::remote::system::{NodeId, RemoteActorSystem};
use chrono::{DateTime, Utc, MIN_DATETIME};

use futures::FutureExt;
use std::cmp::Ordering;
use std::collections::HashMap;

use std::ops::Add;

use std::sync::Arc;

use std::time::{Duration, Instant};

pub struct Heartbeat {
    config: Arc<HeartbeatConfig>,
    system: Option<RemoteActorSystem>,
    heartbeat_timer: Option<Timer>,
    last_heartbeat: Option<DateTime<Utc>>,
    node_pings: HashMap<NodeId, NodePing>,
}

pub struct HeartbeatConfig {
    pub interval: Duration,
    pub ping_timeout: Duration,
    pub unhealthy_node_heartbeat_timeout: Duration,
    pub terminated_node_heartbeat_timeout: Duration,
}

impl Heartbeat {
    pub async fn start(
        node_tag: &str,
        config: HeartbeatConfig,
        sys: &ActorSystem,
    ) -> LocalActorRef<Heartbeat> {
        let config = Arc::new(config);
        Heartbeat {
            config,
            system: None,
            heartbeat_timer: None,
            last_heartbeat: None,
            node_pings: HashMap::new(),
        }
        .into_actor(Some(format!("heartbeat-{}", &node_tag)), sys)
        .await
        .expect("heartbeat actor")
    }
}

#[async_trait]
impl Handler<SetRemote> for Heartbeat {
    async fn handle(&mut self, message: SetRemote, ctx: &mut ActorContext) {
        self.system = Some(message.0);

        debug!(
            "starting heartbeat timer (tick duration={} millis), node_id={}",
            &self.config.interval.as_millis(),
            self.system.as_ref().unwrap().node_id()
        );

        self.heartbeat_timer = Some(Timer::start(
            self.actor_ref(ctx),
            self.config.interval,
            HeartbeatTick,
        ));
    }
}

impl Default for HeartbeatConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_millis(500),
            ping_timeout: Duration::from_secs(15),
            unhealthy_node_heartbeat_timeout: Duration::from_millis(1500),
            terminated_node_heartbeat_timeout: Duration::from_secs(30),
        }
    }
}

#[derive(Clone)]
struct HeartbeatTick;

impl Message for HeartbeatTick {
    type Result = ();
}

impl TimerTick for HeartbeatTick {}

#[derive(Clone)]
pub enum PingResult {
    Ok(Pong, Duration, DateTime<Utc>),
    Timeout,
    Disconnected,
    Err,
}

impl PingResult {
    pub fn is_ok(&self) -> bool {
        match &self {
            PingResult::Ok(..) => true,
            _ => false,
        }
    }
}

pub struct NodePing(pub NodeId, pub PingResult);

impl Message for NodePing {
    type Result = ();
}

impl Actor for Heartbeat {}

#[async_trait]
impl Handler<NodePing> for Heartbeat {
    async fn handle(&mut self, message: NodePing, _ctx: &mut ActorContext) {
        let _ = self.node_pings.insert(message.0, message);
    }
}

#[async_trait]
impl Handler<HeartbeatTick> for Heartbeat {
    async fn handle(&mut self, _msg: HeartbeatTick, _ctx: &mut ActorContext) {
        let system = self.system.as_ref().unwrap();

        let node_tag = system.node_tag();
        let current_node = system.node_id();

        let now = Instant::now();
        let nodes = system.get_nodes().await;

        trace!(target: "Heartbeat", "heartbeat tick, node_id={}, node_tag={}, nodes={}, healthy_nodes={}",
            &current_node, &node_tag, &nodes.len(),
            &nodes
              .iter()
              .filter(|n| n.status == NodeStatus::Healthy)
              .count()
        );

        let mut updates = vec![];

        for node in nodes {
            if node.id == current_node {
                let mut node = node;
                node.status = NodeStatus::Healthy;
                node.last_heartbeat = Some(Utc::now());
                updates.push(node);

                continue;
            }

            if &node.status == &NodeStatus::Terminated {
                continue;
            }

            let node_id = node.id;
            updates.push(update_node(
                current_node,
                node,
                self.node_pings.get(&node_id).map(|r| r.1.clone()),
                &self.config,
            ));
        }

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

        if self.last_heartbeat.is_some() {
            let oldest_healthy_node = updates
                .iter()
                .filter(|n| n.status != NodeStatus::Unhealthy || n.status != NodeStatus::Terminated)
                .next();

            if let Some(oldest_healthy_node) = oldest_healthy_node {
                if Some(oldest_healthy_node.id) != system.current_leader() {
                    system.update_leader(oldest_healthy_node.id);

                    info!(
                        "[node={}] leader of cluster: {:?}, current_node_tag={}",
                        system.node_id(),
                        &oldest_healthy_node,
                        &node_tag
                    );

                    let id = oldest_healthy_node.id;
                    let sys = system.clone();
                    tokio::spawn(async move {
                        let _ = PubSub::publish_locally(
                            SystemTopic,
                            SystemEvent::Cluster(LeaderChanged(id)),
                            &sys,
                        )
                        .await;
                    });
                }
            }
        }

        system.update_nodes(updates).await;
        self.last_heartbeat = Some(Utc::now())
    }
}

fn update_node(
    node_id: NodeId,
    mut node: RemoteNodeState,
    ping: Option<PingResult>,
    heartbeat_config: &HeartbeatConfig,
) -> RemoteNodeState {
    match &ping {
        None => {}
        Some(ping) => match ping {
            PingResult::Ok(_pong, ping_latency, pong_received_at) => {
                node.last_heartbeat = Some(*pong_received_at);
                node.ping_latency = Some(*ping_latency);
            }
            PingResult::Timeout | PingResult::Disconnected | PingResult::Err => {
                node.ping_latency = None;
            }
        },
    }

    node.status = node_status(
        node_id,
        node.id,
        node.status,
        &node.last_heartbeat,
        ping,
        &heartbeat_config,
    );

    node
}

fn node_status(
    node_id: NodeId,
    peer_node_id: NodeId,
    previous_status: NodeStatus,
    last_heartbeat: &Option<DateTime<Utc>>,
    ping: Option<PingResult>,
    config: &HeartbeatConfig,
) -> NodeStatus {
    match ping {
        Some(PingResult::Ok(_, ping_latency, pong_received_at)) => {
            let time_since_ping = (Utc::now() - pong_received_at).to_std().unwrap();

            if time_since_ping >= config.terminated_node_heartbeat_timeout {
                error!(
                    "[node={}] node_id={} has not pinged in {} millis, marking node as terminated",
                    node_id,
                    peer_node_id,
                    time_since_ping.as_millis()
                );

                NodeStatus::Terminated
            } else if time_since_ping >= config.unhealthy_node_heartbeat_timeout
                || ping_latency > config.unhealthy_node_heartbeat_timeout
            {
                warn!(
                    "[node={}] node_id={} took {}ms to respond to ping, marking as unhealthy - time_since_ping={} millis",
                    node_id,
                    peer_node_id,
                    ping_latency.as_millis(),
                    time_since_ping.as_millis()
                );
                NodeStatus::Unhealthy
            } else {
                if previous_status != NodeStatus::Healthy {
                    info!(
                        "[node={}] remote node_id={} is now healthy",
                        node_id, peer_node_id
                    );
                }

                NodeStatus::Healthy
            }
        }

        None => {
            debug!("node_id={} has not pinged yet", peer_node_id);
            NodeStatus::Joining
        }

        Some(PingResult::Timeout) => {
            let terminated = last_heartbeat.map_or(true, |h| {
                h.add(chrono::Duration::from_std(config.terminated_node_heartbeat_timeout).unwrap())
                    >= Utc::now()
            });

            if terminated {
                error!(target: "Heartbeat", "node_id={} has never responded to ping or it has been longer than {} ms since last successful heartbeat, marking as terminated",
                    peer_node_id, config.terminated_node_heartbeat_timeout.as_millis());
                NodeStatus::Terminated
            } else {
                warn!(target: "Heartbeat", "node_id={} did not respond to ping within {} ms, marking as unhealthy",
                    node_id, config.ping_timeout.as_millis());

                NodeStatus::Unhealthy
            }
        }

        Some(PingResult::Disconnected) => NodeStatus::Terminated,

        Some(PingResult::Err) => {
            if previous_status == NodeStatus::Unhealthy {
                error!(
                    target: "Heartbeat",
                    "error during ping rpc to node={}, marking node as terminated",
                    node_id,
                );
                NodeStatus::Terminated
            } else {
                error!(
                    target: "Heartbeat",
                    "error during ping rpc to node={}, marking node as unhealthy",
                    node_id,
                );
                NodeStatus::Unhealthy
            }
        }
    }
}
