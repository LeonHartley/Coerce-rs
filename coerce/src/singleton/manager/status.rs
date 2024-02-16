use crate::actor::context::ActorContext;
use crate::actor::message::{
    FromBytes, Handler, Message, MessageUnwrapErr, MessageWrapErr, ToBytes,
};
use crate::remote::system::NodeId;
use crate::singleton::factory::SingletonFactory;
use crate::singleton::manager::{Manager, State};
use crate::singleton::proto::singleton as proto;
use protobuf::EnumOrUnknown;

pub struct GetStatus {
    source_node_id: NodeId,
}

pub struct ManagerStatus {
    singleton_state: SingletonState,
}

pub enum SingletonState {
    Joining,
    Idle,
    Starting,
    Running,
    Stopping,
}

#[async_trait]
impl<F: SingletonFactory> Handler<GetStatus> for Manager<F> {
    async fn handle(&mut self, message: GetStatus, _ctx: &mut ActorContext) -> ManagerStatus {
        debug!(
            "Singleton manager status request from node_id={}",
            message.source_node_id
        );

        let singleton_state = match &self.state {
            State::Joining { .. } => SingletonState::Joining,
            State::Idle => SingletonState::Idle,
            State::Starting { .. } => SingletonState::Starting,
            State::Running { .. } => SingletonState::Running,
            State::Stopping { .. } => SingletonState::Stopping,
        };

        ManagerStatus { singleton_state }
    }
}

impl Message for GetStatus {
    type Result = ManagerStatus;

    fn as_bytes(&self) -> Result<Vec<u8>, MessageWrapErr> {
        proto::GetStatus {
            source_node_id: self.source_node_id,
            ..Default::default()
        }
        .to_bytes()
    }

    fn read_remote_result(buf: Vec<u8>) -> Result<Self::Result, MessageUnwrapErr> {
        proto::ManagerStatus::from_bytes(buf).map(|s| ManagerStatus {
            singleton_state: s.singleton_state.unwrap().into(),
        })
    }

    fn write_remote_result(res: Self::Result) -> Result<Vec<u8>, MessageWrapErr> {
        proto::ManagerStatus {
            singleton_state: EnumOrUnknown::new(res.singleton_state.into()),
            ..Default::default()
        }
        .to_bytes()
    }
}

impl Into<proto::SingletonState> for SingletonState {
    fn into(self) -> proto::SingletonState {
        match self {
            SingletonState::Joining => proto::SingletonState::JOINING,
            SingletonState::Idle => proto::SingletonState::IDLE,
            SingletonState::Starting => proto::SingletonState::STARTING,
            SingletonState::Running => proto::SingletonState::RUNNING,
            SingletonState::Stopping => proto::SingletonState::STOPPING,
        }
    }
}

impl From<proto::SingletonState> for SingletonState {
    fn from(value: proto::SingletonState) -> Self {
        match value {
            proto::SingletonState::JOINING => Self::Joining,
            proto::SingletonState::IDLE => Self::Idle,
            proto::SingletonState::STARTING => Self::Starting,
            proto::SingletonState::RUNNING => Self::Running,
            proto::SingletonState::STOPPING => Self::Stopping,
        }
    }
}
