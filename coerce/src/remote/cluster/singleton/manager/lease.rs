use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message};
use crate::actor::Actor;
use crate::remote::cluster::singleton::factory::SingletonFactory;
use crate::remote::cluster::singleton::manager::status::SingletonState;
use crate::remote::cluster::singleton::manager::{Manager, State};
use crate::remote::system::NodeId;

#[derive(Clone)]
pub struct RequestLease {
    pub source_node_id: NodeId,
}

impl Message for RequestLease {
    type Result = ();
}

pub struct LeaseAck {
    pub source_node_id: NodeId,
}

impl Message for LeaseAck {
    type Result = ();
}

pub struct LeaseNack {
    singleton_state: SingletonState,
}

#[async_trait]
impl<F: SingletonFactory> Handler<RequestLease> for Manager<F> {
    async fn handle(&mut self, message: RequestLease, ctx: &mut ActorContext) {
        if !self.state.is_running() {
            self.grant_lease(message.source_node_id).await;
        } else {
            info!(
                "node_id={} already running singleton={}, stopping",
                self.node_id,
                F::Actor::type_name()
            );

            self.begin_stopping(message.source_node_id);
        }
    }
}

#[async_trait]
impl<F: SingletonFactory> Handler<LeaseAck> for Manager<F> {
    async fn handle(&mut self, message: LeaseAck, ctx: &mut ActorContext) {
        self.on_lease_ack(message.source_node_id).await;
    }
}

impl<F: SingletonFactory> Manager<F> {
    pub async fn request_lease(&self) {
        let request = RequestLease {
            source_node_id: self.node_id,
        };

        for manager in self.managers.values() {
            if let Err(e) = manager.notify(request.clone()).await {
                warn!(
                    "Failed to request lease from node={:?}, e={}",
                    manager.node_id(),
                    e
                )
            }
        }
    }

    pub async fn grant_lease(&self, node_id: NodeId) {
        self.notify_manager(
            node_id,
            LeaseAck {
                source_node_id: self.node_id,
            },
        )
    }

    pub async fn on_lease_ack(&mut self, node_id: NodeId) {
        match &mut self.state {
            State::Starting { acknowledged_nodes } => {
                acknowledged_nodes.insert(node_id);

                // TODO: Can we do it with a quorum rather than *all managers*?
                if acknowledged_nodes.len() == self.managers.len() {
                    info!("starting singleton on node={}", self.node_id);
                }
            }

            _ => {}
        }
    }

    pub async fn deny_lease(&self, node_id: NodeId) {}
}
