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
use crate::remote::cluster::sharding::host::StartEntity;
use crate::remote::cluster::sharding::proto::sharding::ShardStateSnapshot as ShardProtoSnapshot;
use crate::remote::handler::ActorHandler;
use crate::remote::net::message::SessionEvent;
use crate::remote::net::proto::network::ClientResult;

use crate::remote::system::NodeId;
use protobuf::Message as ProtoMessage;
use std::collections::HashMap;
use tokio::sync::oneshot;

pub struct Shard {
    shard_id: ShardId,
    handler: BoxedActorHandler,
    persistent_entities: bool,
    entities: HashMap<String, Entity>,
}

impl Shard {
    pub fn new(shard_id: ShardId, handler: BoxedActorHandler) -> Shard {
        let persistent_entities = false;
        Shard {
            shard_id,
            handler,
            persistent_entities,
            entities: HashMap::new(),
        }
    }
}

#[derive(Clone)]
struct Entity {
    actor_id: String,
    recipe: Vec<u8>,
}

struct ShardStateSnapshot {
    shard_id: ShardId,
    node_id: NodeId,
    entities: Vec<Entity>,
}

#[async_trait]
impl PersistentActor for Shard {
    fn configure(types: &mut JournalTypes<Self>) {
        types.snapshot::<ShardStateSnapshot>("ShardStateSnapshot");
    }

    async fn post_recovery(&mut self, _ctx: &mut ActorContext) {
        debug!("started shard#{}", self.shard_id);
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

        self.snapshot(snapshot, ctx).await
    }
}

#[async_trait]
impl RecoverSnapshot<ShardStateSnapshot> for Shard {
    async fn recover(&mut self, snapshot: ShardStateSnapshot, ctx: &mut ActorContext) {
        for entity in snapshot.entities {
            self.start_entity(entity.actor_id, &entity.recipe, ctx)
                .await;
        }
    }
}

impl Shard {
    async fn start_entity(
        &mut self,
        actor_id: ActorId,
        recipe: &Vec<u8>,
        ctx: &mut ActorContext,
    ) -> Result<BoxedActorRef, ActorRefErr> {
        let entity = self
            .handler
            .create(Some(actor_id.clone()), recipe.clone(), Some(ctx))
            .await;

        if self.persistent_entities && entity.is_ok() {
            let create_entity = Entity {
                actor_id: actor_id.clone(),
                recipe: recipe.clone(),
            };

            self.entities.insert(actor_id, create_entity);
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
}

#[async_trait]
impl Handler<StartEntity> for Shard {
    async fn handle(&mut self, message: StartEntity, ctx: &mut ActorContext) {
        /*  TODO: `start_entity` should be done asynchronously, any messages sent while the actor is
                   starting should be buffered and emitted once the Shard receives confirmation that the actor was created
        */

        let _res = self
            .start_entity(message.actor_id, message.recipe.as_ref(), ctx)
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
            warn!(
                "no message handler configured for message={}, actor_type={}, actor_id={}",
                message.message_type,
                self.handler.actor_type_name(),
                &actor_id
            );
            result_channel.map(|m| m.send(Err(ActorRefErr::ActorUnavailable)));
            return;
        }

        let handler = handler.unwrap();
        let actor = match actor {
            Some(actor) => actor,
            None => match message.recipe {
                /*  TODO: `start_entity` should be done asynchronously, any messages sent while the actor is
                          starting should be buffered and emitted once the Shard receives confirmation that the actor was created
                */
                Some(recipe) => match self.start_entity(actor_id, recipe.as_ref(), ctx).await {
                    Ok(actor) => actor,
                    Err(_err) => {
                        // TODO: Send actor could not be created err
                        result_channel.map(|m| m.send(Err(ActorRefErr::ActorUnavailable)));
                        return;
                    }
                },
                None => {
                    // TODO: send actor doesnt exist (and cannot be created) err
                    result_channel.map(|m| m.send(Err(ActorRefErr::ActorUnavailable)));
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

impl Snapshot for ShardStateSnapshot {
    fn into_remote_envelope(self) -> Result<Envelope<Self>, MessageWrapErr> {
        let proto = ShardProtoSnapshot {
            shard_id: self.shard_id,
            node_id: self.node_id,
            entities: Default::default(),
            ..Default::default()
        };

        proto.write_to_bytes().map_or_else(
            |_e| Err(MessageWrapErr::SerializationErr),
            |s| Ok(Envelope::Remote(s)),
        )
    }

    fn from_remote_envelope(b: Vec<u8>) -> Result<Self, MessageUnwrapErr> {
        let proto = ShardProtoSnapshot::parse_from_bytes(&b);

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
                        })
                        .collect(),
                    node_id: s.node_id,
                    shard_id: s.shard_id,
                })
            },
        )
    }
}
