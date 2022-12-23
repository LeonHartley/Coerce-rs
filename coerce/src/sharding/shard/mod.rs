use crate::actor::context::ActorContext;
use crate::actor::message::{Envelope, Handler, Message, MessageUnwrapErr, MessageWrapErr};
use crate::actor::{Actor, ActorId, ActorRefErr, BoxedActorRef, CoreActorRef, IntoActorId};
use crate::persistent::journal::snapshot::Snapshot;
use crate::persistent::journal::types::JournalTypes;
use crate::persistent::journal::PersistErr;
use crate::persistent::{PersistentActor, Recover, RecoverSnapshot};
use crate::remote::actor::{BoxedActorHandler, BoxedMessageHandler};
use crate::remote::system::NodeId;
use crate::sharding::coordinator::ShardId;
use crate::sharding::host::request::{EntityRequest, RemoteEntityRequest};
use crate::sharding::proto::sharding as proto;
use crate::sharding::shard::message::{
    EntityStartResult, PassivateEntity, RemoveEntity, StartEntity,
};
use crate::sharding::shard::recovery::ShardStateSnapshot;
use chrono::{DateTime, Utc};
use futures::SinkExt;
use protobuf::Message as ProtoMessage;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::mem;
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio::sync::oneshot::Sender;

pub mod message;
pub(crate) mod passivation;
pub(crate) mod recovery;
pub(crate) mod stats;

pub type RecipeRef = Arc<Vec<u8>>;

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

struct BufferedReq {
    handler: BoxedMessageHandler,
    message: Vec<u8>,
    result_channel: Option<Sender<Result<Vec<u8>, ActorRefErr>>>,
}

enum EntityState {
    Idle,
    Starting { request_buffer: Vec<BufferedReq> },
    Active(BoxedActorRef),
    Passivated,
}

impl Clone for EntityState {
    fn clone(&self) -> Self {
        Self::Idle
    }
}

impl Debug for EntityState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self {
            EntityState::Idle => write!(f, "EntityState::Idle"),
            EntityState::Starting { request_buffer } => write!(
                f,
                "EntityState::Starting(request_buffer: {} messages)",
                request_buffer.len()
            ),
            EntityState::Active(actor_ref) => {
                write!(f, "EntityState::Active(actor: {:?})", actor_ref)
            }
            EntityState::Passivated => write!(f, "EntityState::Passivated"),
        }
    }
}

impl EntityState {
    pub fn starting(request: Option<BufferedReq>) -> EntityState {
        EntityState::Starting {
            request_buffer: request.map(|r| vec![r]).unwrap_or_else(|| vec![]),
        }
    }

    pub fn is_starting(&self) -> bool {
        matches!(&self, EntityState::Starting { .. })
    }

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
    recipe: RecipeRef,
    state: EntityState,
    last_request: DateTime<Utc>,
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
        let node_id = ctx.system().remote().node_id();

        info!(
            "started shard#{} on node_id={}, recovered_entities={}",
            self.shard_id, node_id, recovered_entities
        );

        if recovered_entities > 0 {
            self.recover_entities(ctx).await;
        }

        if self.entity_passivation {
            // TODO: Start entity passivation worker
        }
    }

    async fn on_child_stopped(&mut self, id: &ActorId, _ctx: &mut ActorContext) {
        info!("child stopped, id={}", id);
    }
}

impl Shard {
    fn get_or_create_entity(
        &mut self,
        actor_id: &ActorId,
        recipe: Option<RecipeRef>,
    ) -> Option<&mut Entity> {
        let entry = self.entities.entry(actor_id.clone());
        match entry {
            Entry::Occupied(entry) => Some(entry.into_mut()),
            Entry::Vacant(vacant_entry) => {
                if let Some(recipe) = recipe {
                    let entity = Entity {
                        actor_id: actor_id.clone(),
                        recipe: recipe.clone(),
                        state: EntityState::Idle,
                        last_request: Utc::now(),
                    };

                    Some(vacant_entry.insert(entity))
                } else {
                    None
                }
            }
        }
    }

    async fn recover_entities(&mut self, ctx: &mut ActorContext) {
        let entities = self.get_active_entities();
        for entity in entities {
            let _ = self
                .start_entity(entity.actor_id.clone(), entity.recipe, ctx, true)
                .await;
        }
    }

    fn get_active_entities(&self) -> Vec<Entity> {
        self.entities
            .values()
            .filter(|e| e.state.is_starting())
            .cloned()
            .collect()
    }
}

#[async_trait]
impl Handler<EntityStartResult> for Shard {
    async fn handle(&mut self, message: EntityStartResult, ctx: &mut ActorContext) {
        let EntityStartResult {
            actor_id,
            result,
            is_shard_recovery,
        } = message;

        match result {
            Ok(actor_ref) => {
                if let Some(previous) = ctx.add_child_ref(actor_ref.clone()) {
                    warn!("spawned entity (id={}) was not removed correctly, ChildRef still exists, {:?}", &actor_id, &previous);
                }

                if let Some(entity) = self.entities.get_mut(&actor_id) {
                    let previous = mem::replace(&mut entity.state, EntityState::Active(actor_ref));
                    match previous {
                        EntityState::Starting { request_buffer } => {
                            debug!(
                                "entity({}) request buffer: {} message(s)",
                                &entity.actor_id,
                                request_buffer.len()
                            );

                            for msg in request_buffer {
                                entity.request(msg.handler, msg.message, msg.result_channel);
                            }
                        }
                        state => {
                            warn!("spawned entity (id={}) state(previous={:?}) was unexpected (expected: `Starting`)", &actor_id, &state);
                            // TODO: anything to cleanup?
                        }
                    }
                }

                debug!(
                    "spawned entity as a child of Shard#{}, actor_id={}, total_hosted_entities={}, is_shard_recovery={}",
                    &self.shard_id,
                    &actor_id,
                    &ctx.supervised_count(),
                    is_shard_recovery,
                );
            }
            Err(e) => {
                // TODO: we should respond to all buffered requests with a failure

                error!(
                    "failed to spawn entity as a child of Shard#{}, actor_id={}, error={}, is_shard_recovery={}",
                    &self.shard_id, &actor_id, e, is_shard_recovery
                )
            }
        }
    }
}

impl Shard {
    async fn start_entity(
        &mut self,
        actor_id: ActorId,
        recipe: RecipeRef,
        ctx: &mut ActorContext,
        is_shard_recovery: bool,
    ) {
        let start_entity = StartEntity {
            actor_id: actor_id.clone(),
            recipe: recipe.clone(),
        };

        if !is_shard_recovery && self.persistent_entities {
            let _persist_res = self.persist(&start_entity, ctx).await;
        }

        let handler = self.handler.new_boxed();
        let system = ctx.system().clone();
        let self_ref = self.actor_ref(ctx);

        tokio::spawn(async move {
            let result = handler
                .create(
                    Some(actor_id.clone()),
                    &start_entity.recipe,
                    Some(self_ref.clone().into()),
                    Some(&system),
                )
                .await;

            let _ = self_ref.notify(EntityStartResult {
                actor_id,
                result,
                is_shard_recovery,
            });
        });
    }
}

#[async_trait]
impl Handler<StartEntity> for Shard {
    async fn handle(&mut self, message: StartEntity, ctx: &mut ActorContext) {
        self.entities.insert(
            message.actor_id.clone(),
            Entity {
                actor_id: message.actor_id.clone(),
                recipe: message.recipe.clone(),
                state: EntityState::starting(None)/*TODO: if the previous actor was passivated, we should reflect that in the status so the actor is not restarted during recovery*/,
                last_request: Utc::now(),
            },
        );

        self.start_entity(message.actor_id, message.recipe, ctx, false)
            .await;
    }
}

impl Entity {
    pub fn request(
        &mut self,
        handler: BoxedMessageHandler,
        message: Vec<u8>,
        result_channel: Option<Sender<Result<Vec<u8>, ActorRefErr>>>,
    ) {
        match &mut self.state {
            EntityState::Passivated | EntityState::Idle => {
                error!("request attempt for an actor that has not been marked as `Starting`");
                result_channel.map(|c| c.send(Err(ActorRefErr::ActorUnavailable)));
            }

            EntityState::Starting { request_buffer } => {
                request_buffer.push(BufferedReq {
                    handler,
                    message,
                    result_channel,
                });

                debug!(
                    "entity(id={}) starting, request buffered (total_buffered={})",
                    &self.actor_id,
                    request_buffer.len()
                );
            }

            EntityState::Active(actor_ref) => {
                let message = message;
                let actor_ref = actor_ref.clone();
                tokio::spawn(async move {
                    handler
                        .handle_direct(&actor_ref, &message, result_channel)
                        .await;
                });
            }
        }
    }
}

#[async_trait]
impl Handler<EntityRequest> for Shard {
    async fn handle(&mut self, message: EntityRequest, ctx: &mut ActorContext) {
        let system = ctx.system().remote();
        let handler = system.config().message_handler(&message.message_type);

        let actor_id = message.actor_id;
        let result_channel = message.result_channel;
        let message_bytes = message.message;

        debug!(
            "entity request, node={}, actor_id={}, shard_id={}, msg_len={}, type={}",
            system.node_id(),
            &actor_id,
            self.shard_id,
            message_bytes.len(),
            &message.message_type,
        );

        if handler.is_none() {
            let message_type = message.message_type;
            let actor_type = self.handler.actor_type_name().to_string();

            warn!(
                "no message handler configured for message={}, actor_type={}, actor_id={}",
                &message_type, &actor_type, &actor_id
            );

            result_channel.map(|m| {
                let actor_id = actor_id;
                let _ = m.send(Err(ActorRefErr::NotSupported {
                    actor_id,
                    message_type,
                    actor_type,
                }));
            });

            return;
        }

        let handler = handler.unwrap();
        let entity = self.entities.entry(actor_id.clone());
        let recipe = message.recipe;
        let message = message_bytes;
        match entity {
            Entry::Occupied(entity) => {
                let entity = entity.into_mut();
                entity.request(handler, message, result_channel);
            }

            Entry::Vacant(entry) => {
                if let Some(recipe) = recipe {
                    let _ = entry.insert(Entity {
                        actor_id: actor_id.clone(),
                        recipe: recipe.clone(),
                        state: EntityState::starting(Some(BufferedReq {
                            handler,
                            message,
                            result_channel,
                        })),
                        last_request: Utc::now(),
                    });

                    self.start_entity(actor_id.clone(), recipe, ctx, false)
                        .await;
                } else {
                    result_channel.map(|m| {
                        let actor_id = actor_id;
                        let _ = m.send(Err(ActorRefErr::NotFound(actor_id)));
                    });
                }
            }
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
