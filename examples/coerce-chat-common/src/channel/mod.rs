use coerce_rt::actor::Actor;
use std::collections::HashMap;
use uuid::Uuid;

#[derive(Serialize, Deserialize)]
pub struct ChannelMeta {
    id: Uuid,
    name: String,
}

#[derive(Serialize, Deserialize)]
pub enum ChannelRole {
    Guest,
    Admin,
}

#[derive(Serialize, Deserialize)]
pub struct Channel {
    id: Uuid,
    name: String,
    members: HashMap<Uuid, ChannelMember>,
}

#[derive(Serialize, Deserialize)]
pub struct ChannelMember {
    user_id: Uuid,
    role: ChannelRole,
}

impl Actor for Channel {}

impl Channel {
    pub fn new(name: String, creator_id: Uuid) -> Channel {
        let id = Uuid::new_v4();

        let mut members = HashMap::new();
        members.insert(
            creator_id,
            ChannelMember::new(creator_id, ChannelRole::Admin),
        );

        Channel { id, name, members }
    }
}

impl ChannelMember {
    pub fn new(user_id: Uuid, role: ChannelRole) -> ChannelMember {
        ChannelMember { user_id, role }
    }
}
