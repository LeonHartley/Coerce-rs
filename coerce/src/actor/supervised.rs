//! Actor supervision and child spawning

use std::collections::HashMap;

use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message};
use crate::actor::scheduler::{start_actor, ActorType};
use crate::actor::system::ActorSystem;
use crate::actor::{
    Actor, ActorId, ActorPath, ActorRefErr, BoxedActorRef, CoreActorRef, LocalActorRef,
};

#[derive(Debug)]
pub struct Supervised {
    pub actor_id: ActorId,
    pub path: ActorPath,
    pub children: HashMap<ActorId, ChildRef>,
}

impl Supervised {
    pub fn new(actor_id: ActorId, path: ActorPath) -> Supervised {
        Self {
            actor_id,
            path,
            children: HashMap::new(),
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub enum ChildType {
    Spawned,
    Attached,
}

#[derive(Debug, Clone)]
pub struct ChildRef {
    child_type: ChildType,
    actor_ref: BoxedActorRef,
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
            self.path.clone(),
        );

        self.children
            .insert(id.clone(), ChildRef::spawned(actor_ref.clone().into()));

        match rx.await {
            Ok(_) => Ok(actor_ref),
            Err(e) => {
                error!("error spawning supervised actor (id={}) {}", &id, e);
                Err(ActorRefErr::ActorStartFailed)
            }
        }
    }

    pub fn spawn_deferred<A: Actor>(
        &mut self,
        id: ActorId,
        actor: A,
        system: ActorSystem,
        parent_ref: BoxedActorRef,
    ) -> Result<LocalActorRef<A>, ActorRefErr> {
        if let Some(_) = self.children.get(&id) {
            return Err(ActorRefErr::AlreadyExists(id));
        }

        let actor_ref = start_actor(
            actor,
            id.clone(),
            ActorType::Anonymous,
            None,
            Some(system),
            Some(parent_ref),
            self.path.clone(),
        );

        self.children
            .insert(id.clone(), ChildRef::spawned(actor_ref.clone().into()));

        Ok(actor_ref)
    }

    pub fn count(&self) -> usize {
        self.children.len()
    }

    pub fn child<A: Actor>(&self, id: &ActorId) -> Option<LocalActorRef<A>> {
        self.children.get(id).and_then(|a| a.actor_ref().as_actor())
    }

    pub fn child_boxed(&self, id: &ActorId) -> Option<BoxedActorRef> {
        self.children.get(id).map(|a| a.actor_ref.clone())
    }

    pub fn attach_child_ref(&mut self, boxed_ref: BoxedActorRef) {
        self.children
            .insert(boxed_ref.actor_id().clone(), ChildRef::attached(boxed_ref));
    }

    pub fn add_child_ref(&mut self, boxed_ref: BoxedActorRef) -> Option<ChildRef> {
        self.children
            .insert(boxed_ref.actor_id().clone(), ChildRef::spawned(boxed_ref))
    }

    pub async fn stop_all(&mut self) {
        let stop_results = futures::future::join_all(
            self.children
                .iter()
                .map(|(id, actor)| async move { (id.clone(), actor.actor_ref.stop().await) }),
        )
        .await;

        for (actor_id, stop_result) in stop_results {
            match stop_result {
                Ok(_) => {
                    trace!(actor_id = actor_id.as_ref(), "actor stopped");
                    self.children.remove(&actor_id);
                }
                Err(e) => match e {
                    ActorRefErr::InvalidRef => {},
                    e => {
                        warn!(actor_id = actor_id.as_ref(), error = format!("{}", e), "failed to stop child");
                    }
                },
            }
        }
        
        let n = self.children.len();
        trace!(actor_id = self.actor_id.as_ref(), total_children = n, "all child actors stopped");
    }

    pub async fn on_child_stopped(&mut self, id: &ActorId) {
        if let Some(_) = self.children.remove(id) {
            trace!(actor_id = id.as_ref(), "child actor stopped");
        } else {
            trace!(actor_id = id.as_ref(), "unknown child actor stopped");
        }
    }
}

impl ChildRef {
    pub fn actor_ref(&self) -> &BoxedActorRef {
        &self.actor_ref
    }

    pub fn is_attached(&self) -> bool {
        matches!(&self.child_type, ChildType::Attached)
    }

    pub fn spawned(actor_ref: BoxedActorRef) -> Self {
        ChildRef {
            child_type: ChildType::Spawned,
            actor_ref,
        }
    }

    pub fn attached(actor_ref: BoxedActorRef) -> Self {
        ChildRef {
            child_type: ChildType::Attached,
            actor_ref,
        }
    }
}
