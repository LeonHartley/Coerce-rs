use crate::storage::StorageErr;
use coerce::actor::message::{MessageUnwrapErr, MessageWrapErr};
use coerce::actor::ActorRefErr;

#[derive(Debug)]
pub enum Error {
    NotReady,
    Storage(StorageErr),
    ActorRef(ActorRefErr),
    Deserialisation(MessageUnwrapErr),
    Serialisation(MessageWrapErr),
}

impl From<StorageErr> for Error {
    fn from(value: StorageErr) -> Self {
        Self::Storage(value)
    }
}
