use crate::channel::ChannelMeta;
use coerce::actor::Actor;
use std::collections::{BTreeSet, HashMap};
use uuid::Uuid;

#[derive(Serialize, Deserialize)]
pub struct User {
    id: Uuid,
    name: &'static str,
    avatar: Option<Vec<u8>>,
    friends: BTreeSet<Uuid>,
    channels: HashMap<Uuid, ChannelMeta>,
    inbox: HashMap<Uuid, UserMessage>,
}

#[derive(Serialize, Deserialize)]
pub struct UserMessage {
    id: Uuid,
    message: &'static str,
}

impl Actor for User {}
