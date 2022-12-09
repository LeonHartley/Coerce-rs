use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message};
use crate::actor::scheduler::timer::{Timer, TimerTick};
use crate::actor::system::ActorSystem;
use crate::actor::{Actor, IntoActor, LocalActorRef};
use crate::remote::actor::message::{NodeTerminated, SetRemote};
use crate::remote::cluster::node::{NodeStatus, RemoteNodeState};
use crate::remote::net::proto::network::PongEvent;
use crate::remote::stream::pubsub::PubSub;
use crate::remote::stream::system::ClusterEvent::LeaderChanged;
use crate::remote::stream::system::{SystemEvent, SystemTopic};
use crate::remote::system::{NodeId, RemoteActorSystem};
use chrono::{DateTime, Utc, MIN_DATETIME};

use std::cmp::Ordering;
use std::collections::{HashMap, VecDeque};

use std::ops::Add;

use std::time::{Duration, Instant};
use tokio::sync::oneshot::Sender;

pub struct Heartbeat {
    system: Option<RemoteActorSystem>,
    heartbeat_timer: Option<Timer>,
    last_heartbeat: Option<DateTime<Utc>>,
    node_pings: HashMap<NodeId, NodePing>,
    on_next_leader_changed: VecDeque<Sender<NodeId>>,
}

pub struct HeartbeatConfig {
    pub interval: Duration,
    pub ping_timeout: Duration,
    pub unhealthy_node_heartbeat_timeout: Duration,
    pub terminated_node_heartbeat_timeout: Duration,
}

impl Heartbeat {
    pub async fn start(sys: &ActorSystem) -> LocalActorRef<Heartbeat> {
        Heartbeat {
            system: None,
            heartbeat_timer: None,
            last_heartbeat: None,
            node_pings: HashMap::new(),
            on_next_leader_changed: VecDeque::new(),
        }
        .into_actor(Some("heartbeat"), sys)
        .await
        .expect("heartbeat actor")
    }
}

#[async_trait]
impl Handler<SetRemote> for Heartbeat {
    async fn handle(&mut self, message: SetRemote, ctx: &mut ActorContext) {
        let system = message.0;
        let heartbeat_config = system.config().heartbeat_config();
        debug!(
            "starting heartbeat timer (tick duration={} millis), node_id={}",
            heartbeat_config.interval.as_millis(),
            system.node_id()
        );

        self.heartbeat_timer = Some(Timer::start(
            self.actor_ref(ctx),
            heartbeat_config.interval,
            HeartbeatTick,
        ));

        self.system = Some(system);
        let _ = self.actor_ref(ctx).notify(HeartbeatTick);
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

#[derive(Debug, Clone)]
pub enum PingResult {
    Ok(PongEvent, Duration, DateTime<Utc>),
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

#[derive(Debug)]
pub struct NodePing(pub NodeId, pub PingResult);

impl Message for NodePing {
    type Result = ();
}

pub struct OnLeaderChanged(pub Sender<NodeId>);

impl Actor for Heartbeat {}

impl Message for OnLeaderChanged {
    type Result = ();
}

#[async_trait]
impl Handler<NodePing> for Heartbeat {
    async fn handle(&mut self, message: NodePing, _ctx: &mut ActorContext) {
        let _ = self.node_pings.insert(message.0, message);
    }
}

#[async_trait]
impl Handler<NodeTerminated> for Heartbeat {
    async fn handle(&mut self, message: NodeTerminated, ctx: &mut ActorContext) {
        let node_id = message.0;
        if let Some(system) = &self.system {
            let _ = system.registry().send(message).await;
        }

        self.node_pings.remove(&node_id);
        self.handle(HeartbeatTick, ctx).await;
    }
}

#[async_trait]
impl Handler<OnLeaderChanged> for Heartbeat {
    async fn handle(&mut self, message: OnLeaderChanged, _ctx: &mut ActorContext) {
        self.on_next_leader_changed.push_back(message.0);
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

        trace!(
            "heartbeat tick, node_id={}, node_tag={}, nodes={}, healthy_nodes={}",
            &current_node,
            &node_tag,
            &nodes.len(),
            &nodes
                .iter()
                .filter(|n| n.status == NodeStatus::Healthy)
                .count()
        );

        let mut new_leader_id = None;
        let mut updates = vec![];

        for node in nodes {
            if node.id == current_node {
                let mut node = node;
                node.status = NodeStatus::Healthy;
                node.last_heartbeat = Some(Utc::now());
                updates.push(node);

                continue;
            }

            let node_id = node.id;
            updates.push(update_node(
                current_node,
                node,
                self.node_pings.get(&node_id).map(|r| r.1.clone()),
                system.config().heartbeat_config(),
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
            let oldest_healthy_node = updates.iter().filter(|n| n.status.is_healthy()).next();

            match oldest_healthy_node {
                None => {}
                Some(oldest_healthy_node) => {
                    if Some(oldest_healthy_node.id) != system.current_leader() {
                        info!(
                            "[node={}] leader of cluster: {:?}, current_node_tag={}",
                            system.node_id(),
                            oldest_healthy_node,
                            &node_tag
                        );

                        new_leader_id = Some(oldest_healthy_node.id)
                    }
                }
            }
        }

        system.update_nodes(updates).await;
        self.last_heartbeat = Some(Utc::now());

        if let Some(new_leader_id) = new_leader_id {
            self.update_leader(new_leader_id);
        }
    }
}

impl Heartbeat {
    fn update_leader(&mut self, node_id: NodeId) {
        let system = self.system.as_ref().unwrap();
        system.update_leader(node_id);

        let sys = system.clone();
        tokio::spawn(async move {
            let _ = PubSub::publish_locally(
                SystemTopic,
                SystemEvent::Cluster(LeaderChanged(node_id)),
                &sys,
            )
            .await;
        });

        while let Some(on_leader_changed_cb) = self.on_next_leader_changed.pop_front() {
            let _ = on_leader_changed_cb.send(node_id);
        }
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
            } else if time_since_ping >= config.unhealthy_node_heartbeat_timeout {
                warn!(
                    "[node={}] node_id={} hasn't responded to a ping in {} millis, marking as unhealthy",
                    node_id,
                    peer_node_id,
                    time_since_ping.as_millis()
                );

                NodeStatus::Unhealthy
            } else if ping_latency > config.unhealthy_node_heartbeat_timeout {
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
            if previous_status == NodeStatus::Terminated {
                NodeStatus::Terminated
            } else {
                debug!("node_id={} has not pinged yet", peer_node_id);
                NodeStatus::Joining
            }
        }

        Some(PingResult::Timeout) => {
            let terminated = last_heartbeat.map_or(true, |h| {
                h.add(chrono::Duration::from_std(config.terminated_node_heartbeat_timeout).unwrap())
                    >= Utc::now()
            });

            if terminated {
                error!(
                    "node_id={} has not responded in {} ms, marking as terminated",
                    peer_node_id,
                    config.terminated_node_heartbeat_timeout.as_millis()
                );
                NodeStatus::Terminated
            } else {
                warn!(
                    "node_id={} did not respond to ping within {} ms, marking as unhealthy",
                    peer_node_id,
                    config.ping_timeout.as_millis()
                );

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
