use crate::actor::message::{EnvelopeType, Message, MessageUnwrapErr, MessageWrapErr};
use crate::actor::{ActorId, ActorRefErr, BoxedActorRef, IntoActorId};
use crate::sharding::proto::sharding as proto;
use crate::sharding::shard::RecipeRef;
use protobuf::Message as ProtoMessage;
use std::sync::Arc;

pub struct StartEntity {
    pub actor_id: ActorId,
    pub recipe: RecipeRef,
}

pub struct RemoveEntity {
    pub actor_id: ActorId,
}

pub struct PassivateEntity {
    pub actor_id: ActorId,
}

pub struct EntityStartResult {
    pub actor_id: ActorId,
    pub result: Result<BoxedActorRef, ActorRefErr>,
    pub is_shard_recovery: bool,
}

impl Message for EntityStartResult {
    type Result = ();
}

impl Message for StartEntity {
    type Result = ();

    fn as_bytes(&self) -> Result<Vec<u8>, MessageWrapErr> {
        proto::StartEntity {
            actor_id: self.actor_id.to_string(),
            recipe: self.recipe.as_ref().clone(),
            ..Default::default()
        }
        .write_to_bytes()
        .map_err(|_| MessageWrapErr::SerializationErr)
    }

    fn from_bytes(b: Vec<u8>) -> Result<Self, MessageUnwrapErr> {
        proto::StartEntity::parse_from_bytes(&b)
            .map(|r| Self {
                actor_id: r.actor_id.into_actor_id(),
                recipe: Arc::new(r.recipe),
            })
            .map_err(|_e| MessageUnwrapErr::DeserializationErr)
    }

    fn read_remote_result(_: Vec<u8>) -> Result<Self::Result, MessageUnwrapErr> {
        Ok(())
    }

    fn write_remote_result(_res: Self::Result) -> Result<Vec<u8>, MessageWrapErr> {
        Ok(vec![])
    }
}

impl Message for RemoveEntity {
    type Result = ();

    fn as_bytes(&self) -> Result<Vec<u8>, MessageWrapErr> {
        proto::RemoveEntity {
            actor_id: self.actor_id.to_string(),
            ..Default::default()
        }
        .write_to_bytes()
        .map_err(|_| MessageWrapErr::SerializationErr)
    }

    fn from_bytes(b: Vec<u8>) -> Result<Self, MessageUnwrapErr> {
        proto::RemoveEntity::parse_from_bytes(&b)
            .map(|r| Self {
                actor_id: r.actor_id.into_actor_id(),
            })
            .map_err(|_e| MessageUnwrapErr::DeserializationErr)
    }

    fn read_remote_result(_: Vec<u8>) -> Result<Self::Result, MessageUnwrapErr> {
        Ok(())
    }

    fn write_remote_result(_res: Self::Result) -> Result<Vec<u8>, MessageWrapErr> {
        Ok(vec![])
    }
}

impl Message for PassivateEntity {
    type Result = ();

    fn as_bytes(&self) -> Result<Vec<u8>, MessageWrapErr> {
        proto::PassivateEntity {
            actor_id: self.actor_id.to_string(),
            ..Default::default()
        }
        .write_to_bytes()
        .map_err(|_| MessageWrapErr::SerializationErr)
    }

    fn from_bytes(b: Vec<u8>) -> Result<Self, MessageUnwrapErr> {
        proto::PassivateEntity::parse_from_bytes(&b)
            .map(|r| Self {
                actor_id: r.actor_id.into_actor_id(),
            })
            .map_err(|_e| MessageUnwrapErr::DeserializationErr)
    }

    fn read_remote_result(_: Vec<u8>) -> Result<Self::Result, MessageUnwrapErr> {
        Ok(())
    }

    fn write_remote_result(_res: Self::Result) -> Result<Vec<u8>, MessageWrapErr> {
        Ok(vec![])
    }
}
