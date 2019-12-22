use coerce_rt::actor::ActorId;
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug)]
pub enum ClientError {
    ActorUnavailable,
    HandleError,
}

#[derive(Serialize, Deserialize)]
pub enum ClientEvent {
    Result(Uuid, String),
    Err(Uuid, ClientError),
    Ping(Uuid),
    Pong(Uuid),
}

#[derive(Serialize, Deserialize)]
pub enum SessionEvent {
    Ping(Uuid),
    Pong(Uuid),
    Message {
        id: Uuid,
        handler_type: String,
        actor: ActorId,
        message: String,
    },
}
