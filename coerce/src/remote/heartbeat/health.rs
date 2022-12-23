use crate::actor::context::{ActorContext, ActorStatus};
use crate::actor::message::{Handler, Message};
use crate::actor::{ActorId, ActorPath, BoxedActorRef, CoreActorRef, IntoActorPath};
use crate::remote::cluster::node::RemoteNodeState;
use crate::remote::heartbeat::Heartbeat;
use crate::remote::system::NodeId;
use chrono::{DateTime, Utc};
use futures::future::join_all;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::sync::oneshot;

pub struct SystemHealth {
    pub status: HealthStatus,
    pub node_id: u64,
    pub node_tag: String,
    pub node_version: String,
    pub node_started_at: DateTime<Utc>,
    pub current_leader: Option<NodeId>,
    pub runtime_version: &'static str,
    pub actor_response_times: HashMap<ActorPath, Option<Duration>>,
    pub nodes: Vec<RemoteNodeState>,
}

pub struct RegisterHealthCheck(pub BoxedActorRef);

impl Message for RegisterHealthCheck {
    type Result = ();
}

pub struct RemoveHealthCheck(pub ActorId);

impl Message for RemoveHealthCheck {
    type Result = ();
}

pub struct GetHealth(pub oneshot::Sender<SystemHealth>);

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum HealthStatus {
    Healthy,
    Degraded,
    Unhealthy,
}

impl Message for GetHealth {
    type Result = ();
}

const SLOW_ACTOR_DURATION: Duration = Duration::from_secs(1);

#[async_trait]
impl Handler<GetHealth> for Heartbeat {
    async fn handle(&mut self, m: GetHealth, _ctx: &mut ActorContext) {
        let sender = m.0;
        let system = self.system.as_ref().unwrap().clone();
        let actors = self.health_check_actors.clone();

        tokio::spawn(async move {
            let checks: Vec<(ActorPath, Option<Duration>)> =
                join_all(actors.into_iter().map(|actor| async move {
                    let start = Instant::now();
                    let status = actor.status().await;
                    let time_taken = start.elapsed();
                    let actor_path =
                        format!("{}/{}", actor.actor_path(), actor.actor_id()).into_actor_path();
                    if let Ok(ActorStatus::Started) = status {
                        (actor_path, Some(time_taken))
                    } else {
                        (actor_path, None)
                    }
                }))
                .await;

            let mut errors_exist = false;
            let mut slow_actors_exist = false;
            for check in &checks {
                if check.1.is_none() {
                    errors_exist = true;
                    break;
                } else if let Some(check) = &check.1 {
                    if check > &SLOW_ACTOR_DURATION {
                        /*todo: this should be configurable*/
                        slow_actors_exist = true;
                    }
                }
            }

            let _ = sender.send(SystemHealth {
                node_id: system.node_id(),
                node_tag: system.node_tag().to_string(),
                node_version: system.node_version().to_string(),
                runtime_version: env!("CARGO_PKG_VERSION"),
                node_started_at: *system.started_at(),
                status: if errors_exist {
                    HealthStatus::Unhealthy
                } else if slow_actors_exist {
                    HealthStatus::Degraded
                } else {
                    HealthStatus::Healthy
                },
                actor_response_times: checks.into_iter().map(|n| (n.0, n.1)).collect(),
                nodes: system.get_nodes().await,
                current_leader: system.current_leader(),
            });
        });
    }
}

#[async_trait]
impl Handler<RegisterHealthCheck> for Heartbeat {
    async fn handle(&mut self, message: RegisterHealthCheck, _ctx: &mut ActorContext) {
        let actor = message.0;

        debug!(
            "actor({}/{}) registered for health check",
            actor.actor_path(),
            actor.actor_id()
        );

        self.health_check_actors.push(actor);
    }
}
#[async_trait]
impl Handler<RemoveHealthCheck> for Heartbeat {
    async fn handle(&mut self, message: RemoveHealthCheck, _ctx: &mut ActorContext) {
        let actor_id = message.0;

        debug!("actor({}) removed from health check", actor_id);

        self.health_check_actors
            .retain(|a| a.actor_id() != &actor_id);
    }
}
