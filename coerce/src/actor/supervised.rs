use std::collections::HashMap;

use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message};
use crate::actor::scheduler::{start_actor, ActorType};
use crate::actor::system::ActorSystem;
use crate::actor::{Actor, ActorId, ActorRefErr, BoxedActorRef, CoreActorRef, LocalActorRef};

pub struct Supervised {
    pub actor_id: String,
    pub children: HashMap<ActorId, BoxedActorRef>,
}

impl Supervised {
    pub fn new(actor_id: String) -> Supervised {
        Self {
            actor_id,
            children: HashMap::new(),
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
            supervised.on_child_stopped(&message.0).await;
        }

        self.on_child_stopped(&message.0, ctx).await;
    }
}

impl Supervised {
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
            ActorType::Anonymous,
            Some(tx),
            Some(system),
            Some(parent_ref),
        );

        self.children.insert(id.clone(), actor_ref.clone().into());

        match rx.await {
            Ok(_) => Ok(actor_ref),
            Err(e) => {
                error!("error spawning supervised actor (id={}) {}", &id, e);
                Err(ActorRefErr::ActorStartFailed)
            }
        }
    }

    pub fn count(&self) -> usize {
        self.children.len()
    }

    pub fn child<A: Actor>(&self, id: &ActorId) -> Option<LocalActorRef<A>> {
        self.children
            .get(id)
            .map_or(None, |a| (&a.0.as_any()).downcast_ref::<LocalActorRef<A>>())
            .map(|a| a.clone())
    }

    pub fn child_boxed(&self, id: &ActorId) -> Option<BoxedActorRef> {
        self.children.get(id).map(|a| a.clone())
    }

    pub fn attach_child_ref(&mut self, boxed_ref: BoxedActorRef) {
        self.children
            .insert(boxed_ref.actor_id().clone(), boxed_ref.into());
    }

    pub async fn stop_all(&mut self) {
        let n = self.children.len();
        let stop_results = futures::future::join_all(
            self.children
                .iter()
                .map(|(id, actor)| async move { (id.clone(), actor.stop().await) }),
        )
        .await;

        for (actor_id, stop_result) in stop_results {
            if let Ok(status) = stop_result {
                trace!("actor stopped ({}, status={:?})", actor_id, &status);
                self.children.remove(&actor_id);
            } else {
                match stop_result.unwrap_err() {
                    ActorRefErr::InvalidRef => {
                        warn!("invalid ref, actor_id={} already stopped", &actor_id);
                    }
                    e => {
                        warn!("failed to stop child actor_id={}, err={}", actor_id, e);
                    }
                }
            }
        }

        info!("{} stopped {} child actors", &self.actor_id, n);
    }

    pub async fn on_child_stopped(&mut self, id: &ActorId) {
        if let Some(_) = self.children.remove(id) {
            trace!("child actor (id={}) stopped", id);
        } else {
            trace!("unknown child actor (id={}) stopped", id);
        }
    }
}
