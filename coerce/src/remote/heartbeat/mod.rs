use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message};
use crate::actor::scheduler::timer::{Timer, TimerTick};

use crate::actor::{Actor, IntoActor, LocalActorRef};
use crate::remote::system::RemoteActorSystem;
use std::time::Duration;

#[derive(Default)]
pub struct HeartbeatConfig {
    pub interval: Duration,
    pub inactive_node_heartbeat_timeout: Duration,
    pub terminated_node_heartbeat_timeout: Duration,
}

pub struct Heartbeat {
    config: HeartbeatConfig,
    system: RemoteActorSystem,
    heartbeat_timer: Option<Timer>,
}

impl Heartbeat {
    pub async fn start(
        config: HeartbeatConfig,
        remote_system: &RemoteActorSystem,
    ) -> LocalActorRef<Heartbeat> {
        let system = remote_system.clone();
        Heartbeat {
            config,
            system,
            heartbeat_timer: None,
        }
        .into_actor(None, &remote_system.actor_system())
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

        self.heartbeat_timer = Some(Timer::start::<Heartbeat, HeartbeatTick>(
            ctx.actor_ref(),
            self.config.interval,
            HeartbeatTick,
        ));
    }
}

#[async_trait]
impl Handler<HeartbeatTick> for Heartbeat {
    async fn handle(&mut self, _msg: HeartbeatTick, _ctx: &mut ActorContext) {
        let _node_tag = self.system.node_tag();
        let node_id = self.system.node_id();

        trace!(target: "Heartbeat", "heartbeat tick, node_id={}, node_tag={}", &node_id, &node_id);

        let nodes = self.system.get_nodes().await;
        for _node in nodes {}

        // trace!(target: "Heartbeat", "nodes: {:?}, node_id={}, node_tag={}", &nodes, &node_id, &node_tag);

        // send heartbeat to all nodes
        // and check last heartbeat from other nodes
        // if nodes last heartbeat
    }
}
