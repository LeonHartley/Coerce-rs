use std::collections::HashMap;

use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message};
use crate::actor::scheduler::{start_actor, ActorType};
use crate::actor::system::ActorSystem;
use crate::actor::{Actor, ActorId, ActorRefErr, BoxedActorRef, LocalActorRef};
use slog::Logger;
use std::any::Any;

pub struct Supervised {
    children: HashMap<ActorId, BoxedActorRef>,
    log: Logger,
}

impl Supervised {
    pub fn new(log: Logger) -> Supervised {
        Self {
            children: HashMap::new(),
            log,
        }
    }
}

pub struct Terminated(pub ActorId);

impl Message for Terminated {
    type Result = ();
}

#[async_trait]
impl<A: Actor> Handler<Terminated> for A {
    async fn handle(&mut self, message: Terminated, ctx: &mut ActorContext) {
        if let Some(supervised) = ctx.supervised_mut() {
            supervised.on_child_stopped(&message.0);
        }

        self.on_child_stopped(&message.0, ctx).await;
    }
}

impl Supervised {
    pub fn child<A: Actor>(&self, id: &ActorId) -> Option<LocalActorRef<A>> {
        self.children
            .get(id)
            .map_or(None, |a| (&a.0.as_any()).downcast_ref::<LocalActorRef<A>>())
            .map(|a| a.clone())
    }

    pub fn on_child_stopped(&mut self, id: &ActorId) {
        if let Some(_) = self.children.remove(id) {
            trace!(&self.log, "child actor (id={}) stopped", id);
        } else {
            trace!(&self.log, "unknown child actor (id={}) stopped", id);
        }
    }

    pub async fn spawn<A: Actor>(
        &mut self,
        id: ActorId,
        actor: A,
        system: ActorSystem,
        parent_ref: BoxedActorRef,
    ) -> Result<LocalActorRef<A>, ActorRefErr> {
        if let Some(_) = self.children.get(&id) {
            return Err(ActorRefErr::AlreadyExists(id));
        }

        let (tx, rx) = tokio::sync::oneshot::channel();
        let actor_ref = start_actor(
            actor,
            id.clone(),
            ActorType::Supervised,
            Some(tx),
            Some(system),
            Some(parent_ref),
            None,
        );

        self.children.insert(id, actor_ref.clone().into());
        match rx.await {
            Ok(_) => Ok(actor_ref),
            Err(e) => {
                error!(&self.log, "error spawning supervised actor {}", e);
                Err(ActorRefErr::ActorUnavailable)
            }
        }
    }
}
