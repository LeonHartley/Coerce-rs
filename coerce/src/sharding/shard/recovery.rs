use crate::actor::context::ActorContext;
use crate::actor::message::{Envelope, MessageUnwrapErr, MessageWrapErr};
use crate::actor::IntoActorId;
use crate::persistent::journal::snapshot::Snapshot;
use crate::persistent::journal::PersistErr;
use crate::persistent::{PersistentActor, Recover, RecoverSnapshot};
use crate::remote::system::NodeId;
use crate::sharding::coordinator::ShardId;
use crate::sharding::proto::sharding as proto;
use crate::sharding::shard::message::{PassivateEntity, RemoveEntity, StartEntity};
use crate::sharding::shard::{Entity, EntityState, Shard};
use chrono::Utc;
use protobuf::Message;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

pub struct ShardStateSnapshot {
    shard_id: ShardId,
    node_id: NodeId,
    entities: Vec<Entity>,
}

impl Display for ShardStateSnapshot {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ShardStateSnapshot(shard_id={}, node_id={}, entity_count={})",
            self.shard_id,
            self.node_id,
            self.entities.len()
        )
    }
}

impl Shard {
    async fn save_snapshot(&self, ctx: &mut ActorContext) -> Result<(), PersistErr> {
        let node_id = ctx.system().remote().node_id();
        let shard_id = self.shard_id;
        let snapshot = ShardStateSnapshot {
            node_id,
            shard_id,
            entities: self.entities.values().cloned().collect(),
        };

        info!("saving snapshot - {}", &snapshot);
        self.snapshot(snapshot, ctx).await
    }
}

#[async_trait]
impl Recover<StartEntity> for Shard {
    async fn recover(&mut self, message: StartEntity, _ctx: &mut ActorContext) {
        self.entities.insert(
            message.actor_id.clone(),
            Entity {
                actor_id: message.actor_id,
                recipe: message.recipe,
                state: EntityState::starting(None)/*TODO: if the previous actor was passivated, we should reflect that in the status so the actor is not restarted during recovery*/,
                last_request: Utc::now(),
            },
        );
    }
}

#[async_trait]
impl Recover<PassivateEntity> for Shard {
    async fn recover(&mut self, message: PassivateEntity, _ctx: &mut ActorContext) {
        if let Some(entity) = self.entities.get_mut(&message.actor_id) {
            entity.state = EntityState::Passivated;
        }
    }
}
#[async_trait]
impl Recover<RemoveEntity> for Shard {
    async fn recover(&mut self, message: RemoveEntity, _ctx: &mut ActorContext) {
        self.entities.remove(&message.actor_id);
    }
}

#[async_trait]
impl RecoverSnapshot<ShardStateSnapshot> for Shard {
    async fn recover(&mut self, snapshot: ShardStateSnapshot, _ctx: &mut ActorContext) {
        trace!(
            "shard#{} recovered {} entities",
            &snapshot.shard_id,
            snapshot.entities.len()
        );

        self.entities = snapshot
            .entities
            .into_iter()
            .map(|e| (e.actor_id.clone(), e))
            .collect();
    }
}

impl Snapshot for ShardStateSnapshot {
    fn into_remote_envelope(self) -> Result<Envelope<Self>, MessageWrapErr> {
        let proto = proto::ShardStateSnapshot {
            shard_id: self.shard_id,
            node_id: self.node_id,
            entities: self
                .entities
                .into_iter()
                .map(|e| proto::shard_state_snapshot::Entity {
                    actor_id: e.actor_id.to_string(),
                    recipe: e.recipe.to_vec(),
                    state: match e.state {
                        EntityState::Active(_)
                        | EntityState::Idle
                        | EntityState::Starting { .. } => proto::EntityState::IDLE,
                        EntityState::Passivated => proto::EntityState::PASSIVATED,
                    }
                    .into(),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        };

        proto.write_to_bytes().map_or_else(
            |_e| Err(MessageWrapErr::SerializationErr),
            |s| Ok(Envelope::Remote(s)),
        )
    }

    fn from_remote_envelope(b: Vec<u8>) -> Result<Self, MessageUnwrapErr> {
        let proto = proto::ShardStateSnapshot::parse_from_bytes(&b);

        proto.map_or_else(
            |_e| Err(MessageUnwrapErr::DeserializationErr),
            |s| {
                Ok(Self {
                    entities: s
                        .entities
                        .into_iter()
                        .map(|e| Entity {
                            actor_id: e.actor_id.into_actor_id(),
                            recipe: Arc::new(e.recipe),
                            state: match e.state.unwrap() {
                                proto::EntityState::IDLE | proto::EntityState::ACTIVE => {
                                    EntityState::Idle
                                }
                                proto::EntityState::PASSIVATED => EntityState::Passivated,
                            },
                            last_request: /*TODO: use persisted date*/Utc::now(),
                        })
                        .collect(),
                    node_id: s.node_id,
                    shard_id: s.shard_id,
                })
            },
        )
    }
}
