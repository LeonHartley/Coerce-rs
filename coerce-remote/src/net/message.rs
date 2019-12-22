use coerce_rt::actor::ActorId;
use uuid::Uuid;

#[derive(Serialize, Deserialize)]
pub enum ClientError {
    ActorUnavailable,
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
        identifier: String,
        actor: ActorId,
        message: String,
    },
}
