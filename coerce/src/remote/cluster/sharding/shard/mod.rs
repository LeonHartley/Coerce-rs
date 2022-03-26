use crate::actor::context::{ActorContext, ActorStatus};
use crate::actor::message::{
    Envelope, EnvelopeType, Handler, Message, MessageUnwrapErr, MessageWrapErr,
};
use crate::actor::system::ActorSystem;
use crate::actor::{Actor, ActorId, ActorRefErr, BoxedActorRef};
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
use crate::remote::net::message::SessionEvent;
use crate::remote::net::proto::network::ClientResult;

use crate::actor::scheduler::timer::{Timer, TimerTick};
use crate::remote::system::NodeId;
use chrono::{DateTime, Utc};
use protobuf::Message as ProtoMessage;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::oneshot;

pub(crate) mod stats;

pub struct Shard {
    shard_id: ShardId,
    handler: BoxedActorHandler,
    persistent_entities: bool,
    entity_passivation: bool,
    entity_passivation_tick: Duration,
    entity_passivation_timeout: Duration,
    entity_passivation_timer: Option<Timer>,
    recovered_snapshot: bool,
    entities: HashMap<String, Entity>,
}

impl Shard {
    pub fn new(shard_id: ShardId, handler: BoxedActorHandler, persistent_entities: bool) -> Shard {
        Shard {
            shard_id,
            handler,
            persistent_entities,
            entities: HashMap::new(),
            entity_passivation: false,
            entity_passivation_timeout: Duration::from_secs(60),
            entity_passivation_tick: Duration::from_secs(10),
            entity_passivation_timer: None,
            recovered_snapshot: false,
        }
    }
}

#[derive(Serialize, Deserialize, Copy, Clone)]
enum EntityStatus {
    Active,
    Passivated,
}

impl EntityStatus {
    pub fn is_active(&self) -> bool {
        match &self {
            EntityStatus::Active => true,
            _ => false,
        }
    }
}

#[derive(Clone)]
struct Entity {
    actor_id: String,
    recipe: Vec<u8>,
    status: EntityStatus,
    last_request: DateTime<Utc>,
}

struct ShardStateSnapshot {
    shard_id: ShardId,
    node_id: NodeId,
    entities: Vec<Entity>,
}

#[async_trait]
impl PersistentActor for Shard {
    fn configure(types: &mut JournalTypes<Self>) {
        types
            .snapshot::<ShardStateSnapshot>("ShardStateSnapshot")
            .message::<StartEntity>("StartEntity")
            .message::<PassivateEntity>("PassivateEntity")
            .message::<RemoveEntity>("RemoveEntity");
    }

    async fn post_recovery(&mut self, ctx: &mut ActorContext) {
        let recovered_entities = self.entities.len();
        debug!(
            "started shard#{}, recovered_entities={}",
            self.shard_id, recovered_entities
        );

        if recovered_entities > 0 {
            self.recover_entities(ctx).await;
        }

        if self.entity_passivation {
            self.entity_passivation_timer = Some(Timer::start(
                self.actor_ref(ctx),
                self.entity_passivation_tick,
                PassivationTimerTick,
            ));
        }
    }

    async fn stopped(&mut self, ctx: &mut ActorContext) {
        if let Some(supervised) = ctx.supervised_mut() {
            supervised.stop_all().await;
        }
    }
}

#[derive(Clone)]
struct PassivationTimerTick;

impl Message for PassivationTimerTick {
    type Result = ();
}

impl TimerTick for PassivationTimerTick {}

impl Shard {
    async fn start_entity(
        &mut self,
        actor_id: ActorId,
        recipe: &Vec<u8>,
        ctx: &mut ActorContext,
        is_recovering: bool,
    ) -> Result<BoxedActorRef, ActorRefErr> {
        let entity = self
            .handler
            .create(Some(actor_id.clone()), recipe.clone(), Some(ctx))
            .await;

        self.entities.insert(
            actor_id.clone(),
            Entity {
                actor_id: actor_id.clone(),
                recipe: recipe.clone(),
                status: EntityStatus::Active,
                last_request: Utc::now(),
            },
        );

        if !is_recovering && self.persistent_entities && entity.is_ok() {
            self.persist(
                &StartEntity {
                    actor_id,
                    recipe: recipe.clone(),
                },
                ctx,
            )
            .await;
        }

        entity.map(|entity| {
            debug!(
                "spawned entity, registered it as a child of Shard#{}, hosted_entities={}",
                self.shard_id,
                &ctx.supervised_mut().unwrap().children.len()
            );

            ctx.attach_child_ref(entity.clone());
            entity
        })
    }

    async fn recover_entities(&mut self, ctx: &mut ActorContext) {
        let entities: Vec<Entity> = self
            .entities
            .values()
            .filter(|e| e.status.is_active())
            .cloned()
            .collect();

        for entity in entities {
            self.start_entity(entity.actor_id, &entity.recipe, ctx, true)
                .await;
        }
    }
}

#[async_trait]
impl Handler<PassivationTimerTick> for Shard {
    async fn handle(&mut self, message: PassivationTimerTick, ctx: &mut ActorContext) {}
}

#[async_trait]
impl Handler<StartEntity> for Shard {
    async fn handle(&mut self, message: StartEntity, ctx: &mut ActorContext) {
        /*  TODO: `start_entity` should be done asynchronously, any messages sent while the actor is
                   starting should be buffered and emitted once the Shard receives confirmation that the actor was created
        */

        let _res = self
            .start_entity(message.actor_id, message.recipe.as_ref(), ctx, false)
            .await;
    }
}

#[async_trait]
impl Handler<EntityRequest> for Shard {
    async fn handle(&mut self, message: EntityRequest, ctx: &mut ActorContext) {
        let system = ctx.system().remote();
        let actor = ctx.boxed_child_ref(&message.actor_id);
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
                m.send(Err(ActorRefErr::NotSupported {
                    actor_id,
                    message_type,
                    actor_type,
                }))
            });

            return;
        }

        if self.entity_passivation {
            if let Some(entity) = self.entities.get_mut(&actor_id) {
                entity.last_request = Utc::now();
            }
        }

        let handler = handler.unwrap();
        let actor = match actor {
            Some(actor) => actor,
            None => match message.recipe {
                /*  TODO: `start_entity` should be done asynchronously, any messages sent while the actor is
                          starting should be buffered and emitted once the Shard receives confirmation that the actor was created
                */
                Some(recipe) => match self
                    .start_entity(actor_id, recipe.as_ref(), ctx, false)
                    .await
                {
                    Ok(actor) => actor,
                    Err(_err) => {
                        // TODO: Send actor could not be created err
                        result_channel.map(|m| m.send(Err(ActorRefErr::ActorUnavailable)));
                        return;
                    }
                },
                None => {
                    // TODO: send actor doesnt exist (and cannot be created) err
                    result_channel.map(|m| m.send(Err(ActorRefErr::NotFound(actor_id))));
                    return;
                }
            },
        };

        let message = message.message;
        tokio::spawn(async move {
            handler
                .handle_direct(&actor, &message, result_channel)
                .await;
        });
    }
}

#[async_trait]
impl Handler<RemoteEntityRequest> for Shard {
    async fn handle(&mut self, message: RemoteEntityRequest, ctx: &mut ActorContext) {
        let (tx, rx) = oneshot::channel();

        debug!(
            "remote entity request, node={}, actor_id={}, shard_id={}, origin_node={}",
            ctx.system().remote().node_id(),
            &message.actor_id,
            self.shard_id,
            message.origin_node,
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
                Ok(bytes) => match bytes {
                    Ok(result) => {
                        let message_id = request_id.to_string();
                        let result = SessionEvent::Result(ClientResult {
                            message_id,
                            result,
                            ..Default::default()
                        });

                        system.node_rpc_raw(request_id, result, origin_node).await;
                    }
                    _ => {}
                },
                Err(_) => {}
            };
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
                status: EntityStatus::Active,
                last_request: Utc::now(),
            },
        );
    }
}

#[async_trait]
impl Recover<PassivateEntity> for Shard {
    async fn recover(&mut self, message: PassivateEntity, _ctx: &mut ActorContext) {
        if let Some(entity) = self.entities.get_mut(&message.actor_id) {
            entity.status = EntityStatus::Passivated;
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
    async fn recover(&mut self, snapshot: ShardStateSnapshot, ctx: &mut ActorContext) {
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
                .map(|e| proto::ShardStateSnapshot_Entity {
                    actor_id: e.actor_id,
                    recipe: e.recipe,
                    status: match e.status {
                        EntityStatus::Active => proto::EntityStatus::ACTIVE,
                        EntityStatus::Passivated => proto::EntityStatus::PASSIVATED,
                    },
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
            |e| Err(MessageUnwrapErr::DeserializationErr),
            |s| {
                Ok(Self {
                    entities: s
                        .entities
                        .into_iter()
                        .map(|e| Entity {
                            actor_id: e.actor_id,
                            recipe: e.recipe,
                            status: match e.status {
                                proto::EntityStatus::ACTIVE => EntityStatus::Active,
                                proto::EntityStatus::PASSIVATED => EntityStatus::Passivated,
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
