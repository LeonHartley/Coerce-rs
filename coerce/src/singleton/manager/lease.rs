use crate::actor::context::ActorContext;
use crate::actor::message::{
    FromBytes, Handler, Message, MessageUnwrapErr, MessageWrapErr, ToBytes,
};
use crate::actor::Actor;
use crate::remote::system::{NodeId, RemoteActorSystem};
use crate::singleton::factory::SingletonFactory;
use crate::singleton::manager::status::SingletonState;
use crate::singleton::manager::{Manager, State};
use crate::singleton::proto::singleton as proto;

#[derive(Clone)]
pub struct RequestLease {
    pub source_node_id: NodeId,
}

impl Message for RequestLease {
    type Result = ();

    fn as_bytes(&self) -> Result<Vec<u8>, MessageWrapErr> {
        proto::RequestLease {
            source_node_id: self.source_node_id,
            ..Default::default()
        }
        .to_bytes()
    }

    fn from_bytes(buf: Vec<u8>) -> Result<Self, MessageUnwrapErr> {
        proto::RequestLease::from_bytes(buf).map(|l| Self {
            source_node_id: l.source_node_id,
        })
    }

    fn read_remote_result(_: Vec<u8>) -> Result<Self::Result, MessageUnwrapErr> {
        Ok(())
    }

    fn write_remote_result(_res: Self::Result) -> Result<Vec<u8>, MessageWrapErr> {
        Ok(vec![])
    }
}

#[derive(Clone)]
pub struct LeaseAck {
    pub source_node_id: NodeId,
}

impl Message for LeaseAck {
    type Result = ();

    fn as_bytes(&self) -> Result<Vec<u8>, MessageWrapErr> {
        proto::LeaseAck {
            source_node_id: self.source_node_id,
            ..Default::default()
        }
        .to_bytes()
    }

    fn from_bytes(buf: Vec<u8>) -> Result<Self, MessageUnwrapErr> {
        proto::LeaseAck::from_bytes(buf).map(|l| Self {
            source_node_id: l.source_node_id,
        })
    }

    fn read_remote_result(_: Vec<u8>) -> Result<Self::Result, MessageUnwrapErr> {
        Ok(())
    }

    fn write_remote_result(_res: Self::Result) -> Result<Vec<u8>, MessageWrapErr> {
        Ok(vec![])
    }
}

pub struct LeaseNack {
    singleton_state: SingletonState,
}

#[async_trait]
impl<F: SingletonFactory> Handler<RequestLease> for Manager<F> {
    async fn handle(&mut self, message: RequestLease, ctx: &mut ActorContext) {
        if !self.state.is_running() {
            debug!(
                source_node_id = message.source_node_id,
                "received RequestLease"
            );

            self.grant_lease(message.source_node_id, ctx).await;
        } else {
            debug!(
                node_id = self.node_id,
                singleton = F::Actor::type_name(),
                "singleton already running, stopping",
            );

            self.begin_stopping(message.source_node_id, ctx).await;
        }
    }
}

#[async_trait]
impl<F: SingletonFactory> Handler<LeaseAck> for Manager<F> {
    async fn handle(&mut self, message: LeaseAck, ctx: &mut ActorContext) {
        self.on_lease_ack(message.source_node_id, ctx).await;
    }
}

impl<F: SingletonFactory> Manager<F> {
    pub async fn request_lease(&mut self, ctx: &ActorContext) {
        if self.managers.len() == 0 {
            self.on_all_managers_acknowledged(ctx).await;
            return;
        }

        let request = RequestLease {
            source_node_id: self.node_id,
        };

        let manager_count = self.managers.len();
        debug!(
            source_node_id = self.node_id,
            manager_count = manager_count,
            "requesting lease"
        );
        self.notify_managers(request, ctx).await;
    }

    pub async fn grant_lease(&mut self, node_id: NodeId, ctx: &ActorContext) {
        debug!(target_node_id = node_id, "sending LeaseAck");

        match &mut self.state {
            State::Joining {
                acknowledgement_pending,
            } => {
                debug!(
                    target_node_id = node_id,
                    "manager still joining, buffering acknowledgement until `MemberUp` received"
                );
                acknowledgement_pending.replace(node_id);
            }

            _ => {
                self.notify_manager(
                    node_id,
                    LeaseAck {
                        source_node_id: self.node_id,
                    },
                    ctx,
                )
                .await;
            }
        }
    }

    pub async fn on_lease_ack(&mut self, node_id: NodeId, ctx: &ActorContext) {
        match &mut self.state {
            State::Starting { acknowledged_nodes } => {
                acknowledged_nodes.insert(node_id);

                info!(
                    source_node_id = node_id,
                    total_acknowledgements = acknowledged_nodes.len(),
                    "received LeaseAck",
                );

                // TODO: Can we do it with a quorum rather than *all managers*?
                if acknowledged_nodes.len() == self.managers.len() {
                    self.on_all_managers_acknowledged(ctx).await;
                }
            }

            state => {
                debug!(
                    source_node_id = node_id,
                    node_id = self.node_id,
                    state = format!("{:?}", state),
                    "received LeaseAck but state is not starting"
                );
            }
        }
    }

    pub async fn on_all_managers_acknowledged(&mut self, ctx: &ActorContext) {
        debug!(node_id = self.node_id, "starting singleton");
        self.start_actor(ctx).await;
    }

    pub async fn deny_lease(&self, node_id: NodeId) {}
}
