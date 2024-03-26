use crate::storage::StorageErr;
use coerce::actor::message::{MessageUnwrapErr, MessageWrapErr};
use coerce::actor::ActorRefErr;
use coerce::remote::system::NodeId;

#[derive(Debug)]
pub enum Error {
    NotReady,
    Storage(StorageErr),
    ActorRef(ActorRefErr),
    Deserialisation(MessageUnwrapErr),
    Serialisation(MessageWrapErr),
    LeaderChanged { new_leader_id: NodeId },
}

impl From<StorageErr> for Error {
    fn from(value: StorageErr) -> Self {
        Self::Storage(value)
    }
}
