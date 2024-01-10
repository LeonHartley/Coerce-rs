use crate::actor::context::ActorContext;
use crate::actor::message::{
    FromBytes, Handler, Message, MessageUnwrapErr, MessageWrapErr, ToBytes,
};
use crate::actor::Actor;
use crate::remote::cluster::singleton::factory::SingletonFactory;
use crate::remote::cluster::singleton::manager::status::SingletonState;
use crate::remote::cluster::singleton::manager::{Manager, State};
use crate::remote::cluster::singleton::proto::singleton as proto;
use crate::remote::system::{NodeId, RemoteActorSystem};

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
            info!("received RequestLease from {}", message.source_node_id);
            self.grant_lease(message.source_node_id, ctx).await;
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
        self.on_lease_ack(message.source_node_id, ctx).await;
    }
}

impl<F: SingletonFactory> Manager<F> {
    pub async fn request_lease(&self, ctx: &ActorContext) {
        let request = RequestLease {
            source_node_id: self.node_id,
        };

        info!(source_node_id = self.node_id, "requesting lease");
        self.notify_managers(request, ctx).await;
    }

    pub async fn grant_lease(&self, node_id: NodeId, ctx: &ActorContext) {
        info!(
            "sending LeaseAck to node={} from node={}",
            node_id, self.node_id
        );
        self.notify_manager(
            node_id,
            LeaseAck {
                source_node_id: self.node_id,
            },
            ctx,
        )
        .await;
    }

    pub async fn on_lease_ack(&mut self, node_id: NodeId, ctx: &ActorContext) {
        match &mut self.state {
            State::Starting { acknowledged_nodes } => {
                acknowledged_nodes.insert(node_id);

                info!(
                    source_node_id = node_id,
                    "received LeaseAck, total_acknowledgements={}",
                    acknowledged_nodes.len()
                );

                // TODO: Can we do it with a quorum rather than *all managers*?
                if acknowledged_nodes.len() == self.managers.len() {
                    info!("starting singleton on node={}", self.node_id);
                    self.start_actor(ctx).await;
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

    pub async fn deny_lease(&self, node_id: NodeId) {}
}
