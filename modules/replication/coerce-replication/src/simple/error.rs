use coerce::actor::ActorRefErr;
use coerce::actor::message::{MessageUnwrapErr, MessageWrapErr};
use crate::storage::StorageErr;

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
