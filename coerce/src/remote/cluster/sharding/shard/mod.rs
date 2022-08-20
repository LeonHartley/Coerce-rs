use crate::actor::context::ActorContext;
use crate::actor::message::{Envelope, Handler, MessageUnwrapErr, MessageWrapErr};
use crate::actor::{ActorId, ActorRefErr, BoxedActorRef, CoreActorRef, IntoActorId};
use crate::persistent::journal::snapshot::Snapshot;
use crate::persistent::journal::types::JournalTypes;
use crate::persistent::journal::PersistErr;
use crate::persistent::{PersistentActor, Recover, RecoverSnapshot};
use crate::remote::actor::BoxedActorHandler;
use crate::remote::cluster::sharding::coordinator::ShardId;
use crate::remote::cluster::sharding::host::request::{EntityRequest, RemoteEntityRequest};
use crate::remote::cluster::sharding::host::{PassivateEntity, RemoveEntity, StartEntity};
use crate::remote::cluster::sharding::proto::sharding as proto;
use crate::remote::handler::ActorHandler;

use crate::remote::system::NodeId;
use chrono::{DateTime, Utc};
use protobuf::Message as ProtoMessage;
use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::oneshot;

pub(crate) mod passivation;
pub(crate) mod stats;

pub struct Shard {
    shard_id: ShardId,
    handler: BoxedActorHandler,
    persistent_entities: bool,
    recovered_snapshot: bool,
    entity_passivation: bool,
    entities: HashMap<ActorId, Entity>,
}

impl Shard {
    pub fn new(shard_id: ShardId, handler: BoxedActorHandler, persistent_entities: bool) -> Shard {
        Shard {
            shard_id,
            handler,
            persistent_entities,
            entities: HashMap::new(),
            recovered_snapshot: false,
            entity_passivation: true,
        }
    }
}

impl Drop for Shard {
    fn drop(&mut self) {
        info!("shard#{} dropped", self.shard_id);
    }
}

#[derive(Clone)]
enum EntityState {
    Idle,
    Active(BoxedActorRef),
    Passivated,
}

impl EntityState {
    pub fn is_active(&self) -> bool {
        match &self {
            EntityState::Active(_) => true,
            _ => false,
        }
    }

    pub fn get_actor_ref(&self) -> Option<BoxedActorRef> {
        match &self {
            EntityState::Active(boxed_actor_ref) => Some(boxed_actor_ref.clone()),
            _ => None,
        }
    }
}

#[derive(Clone)]
struct Entity {
    actor_id: ActorId,
    recipe: Arc<Vec<u8>>,
    status: EntityState,
    last_request: DateTime<Utc>,
}

struct ShardStateSnapshot {
    shard_id: ShardId,
    node_id: NodeId,
    entities: Vec<Entity>,
}

struct GetEntities {}

#[async_trait]
impl PersistentActor for Shard {
    fn persistence_key(&self, ctx: &ActorContext) -> String {
        ctx.id().to_string()
    }

    fn configure(types: &mut JournalTypes<Self>) {
        types
            .snapshot::<ShardStateSnapshot>("ShardStateSnapshot")
            .message::<StartEntity>("StartEntity")
            .message::<PassivateEntity>("PassivateEntity")
            .message::<RemoveEntity>("RemoveEntity");
    }

    async fn post_recovery(&mut self, ctx: &mut ActorContext) {
        let recovered_entities = self.entities.len();
        info!(
            "started shard#{}, recovered_entities={}",
            self.shard_id, recovered_entities
        );

        if recovered_entities > 0 {
            self.recover_entities(ctx).await;
        }

        if self.entity_passivation {
            // TODO: Start entity passivation worker
        }
    }
}

impl Shard {
    async fn start_entity(
        &mut self,
        actor_id: ActorId,
        recipe: Arc<Vec<u8>>,
        ctx: &mut ActorContext,
        is_recovering: bool,
    ) -> Result<BoxedActorRef, ActorRefErr> {
        let start_entity = StartEntity {
            actor_id: actor_id.clone(),
            recipe: recipe.clone(),
        };

        if !is_recovering && self.persistent_entities {
            let _persist_res = self.persist(&start_entity, ctx).await;
        }

        let entity = self
            .handler
            .create(
                Some(actor_id.clone()),
                &start_entity.recipe,
                Some(ctx),
                None,
            )
            .await;

        match &entity {
            Ok(actor_ref) => {
                self.entities.insert(
                    actor_id.clone(),
                    Entity {
                        actor_id: actor_id.clone(),
                        recipe,
                        status: EntityState::Active(actor_ref.clone()),
                        last_request: Utc::now(),
                    },
                );

                debug!(
                    "spawned entity as a child of Shard#{}, actor_id={}, total_hosted_entities={}",
                    &self.shard_id,
                    &actor_id,
                    &ctx.supervised_count()
                );
            }
            Err(e) => {
                error!(
                    "failed to spawn entity as a child of Shard#{}, actor_id={}, error={}",
                    &self.shard_id, &actor_id, e
                )
            }
        }

        entity
    }

    async fn recover_entities(&mut self, ctx: &mut ActorContext) {
        let entities = self.get_active_entities();
        for entity in entities {
            if let Err(e) = self
                .start_entity(entity.actor_id.clone(), entity.recipe, ctx, true)
                .await
            {
                warn!(
                    "unable to start recovered entity (actor_id={}, shard_id={}, error={})",
                    &entity.actor_id, &self.shard_id, e
                )
            }
        }
    }

    fn get_active_entities(&self) -> Vec<Entity> {
        self.entities
            .values()
            .filter(|e| e.status.is_active())
            .cloned()
            .collect()
    }
}

#[async_trait]
impl Handler<StartEntity> for Shard {
    async fn handle(&mut self, message: StartEntity, ctx: &mut ActorContext) {
        /*  TODO: `start_entity` should be done asynchronously, any messages sent while the actor is
                   starting should be buffered and emitted once the Shard receives confirmation that the actor was created
        */

        let _res = self
            .start_entity(message.actor_id, message.recipe, ctx, false)
            .await;
    }
}

#[async_trait]
impl Handler<EntityRequest> for Shard {
    async fn handle(&mut self, message: EntityRequest, ctx: &mut ActorContext) {
        let system = ctx.system().remote();
        let handler = system.config().message_handler(&message.message_type);

        let actor_id = message.actor_id;
        let result_channel = message.result_channel;

        debug!(
            "entity request, node={}, actor_id={}, shard_id={}",
            system.node_id(),
            &actor_id,
            self.shard_id
        );

        if handler.is_none() {
            // TODO: send unsupported msg err
            let message_type = message.message_type;
            let actor_type = self.handler.actor_type_name().to_string();

            warn!(
                "no message handler configured for message={}, actor_type={}, actor_id={}",
                &message_type, &actor_type, &actor_id
            );

            result_channel.map(|m| {
                let actor_id = actor_id;
                m.send(Err(ActorRefErr::NotSupported {
                    actor_id,
                    message_type,
                    actor_type,
                }))
            });

            return;
        }

        let handler = handler.unwrap();
        let actor = self.get_or_create(actor_id, message.recipe, ctx).await;
        match actor {
            Ok(actor_ref) => {
                let message = message.message;
                tokio::spawn(async move {
                    handler
                        .handle_direct(&actor_ref, &message, result_channel)
                        .await;
                });
            }
            Err(e) => {
                result_channel.map(|c| c.send(Err(e)));
            }
        }
    }
}

impl Shard {
    async fn get_or_create(
        &mut self,
        actor_id: ActorId,
        recipe: Option<Arc<Vec<u8>>>,
        ctx: &mut ActorContext,
    ) -> Result<BoxedActorRef, ActorRefErr> {
        let entity = self.entities.get_mut(&actor_id);

        let mut recipe = recipe;
        if let Some(entity) = entity {
            if let EntityState::Active(boxed_actor_ref) = &entity.status {
                if boxed_actor_ref.is_valid() {
                    return Ok(boxed_actor_ref.clone());
                }
            }

            // we've seen this actor before, let's create it based on the original recipe.
            recipe = Some(entity.recipe.clone());
        }

        match recipe {
            /*  TODO: `start_entity` should be done asynchronously, any messages sent while the actor is
                      starting should be buffered and emitted once the Shard receives confirmation that the actor was created
            */
            Some(recipe) => match self.start_entity(actor_id, recipe, ctx, false).await {
                Ok(actor) => Ok(actor),
                Err(e) => Err(e),
            },
            None => Err(ActorRefErr::NotFound(actor_id)),
        }
    }
}

#[async_trait]
impl Handler<RemoteEntityRequest> for Shard {
    async fn handle(&mut self, message: RemoteEntityRequest, ctx: &mut ActorContext) {
        let (tx, rx) = oneshot::channel();

        debug!(
            "remote entity request, node={}, actor_id={}, shard_id={}, origin_node={}, request_id={}",
            ctx.system().remote().node_id(),
            &message.actor_id,
            &self.shard_id,
            &message.origin_node,
            &message.request_id,
        );

        let origin_node = message.origin_node;
        let request_id = message.request_id;
        self.handle(
            {
                let mut message: EntityRequest = message.into();
                message.result_channel = Some(tx);
                message
            },
            ctx,
        )
        .await;

        let system = ctx.system().remote_owned();
        tokio::spawn(async move {
            match rx.await {
                Ok(Ok(result)) => {
                    system
                        .notify_raw_rpc_result(request_id, result, origin_node)
                        .await
                }
                Err(_e) => {
                    error!("remote entity request failed, result channel closed");
                    system
                        .notify_rpc_err(request_id, ActorRefErr::ResultChannelClosed, origin_node)
                        .await;
                }
                Ok(Err(e)) => {
                    error!("remote entity request failed, err={}", &e);
                    system.notify_rpc_err(request_id, e, origin_node).await;
                }
            }
        });
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

        info!("saving snapshot");
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
                status: EntityState::Idle,
                last_request: Utc::now(),
            },
        );
    }
}

#[async_trait]
impl Recover<PassivateEntity> for Shard {
    async fn recover(&mut self, message: PassivateEntity, _ctx: &mut ActorContext) {
        if let Some(entity) = self.entities.get_mut(&message.actor_id) {
            entity.status = EntityState::Passivated;
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
                    state: match e.status {
                        EntityState::Active(_) | EntityState::Idle => proto::EntityState::IDLE,
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
                            status: match e.state.unwrap() {
                                proto::EntityState::IDLE | proto::EntityState::ACTIVE => {
                                    EntityState::Idle
                                }
                                proto::EntityState::PASSIVATED => EntityState::Passivated,
                            },
                            last_request: Utc::now(),
                        })
                        .collect(),
                    node_id: s.node_id,
                    shard_id: s.shard_id,
                })
            },
        )
    }
}
