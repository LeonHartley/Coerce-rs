use crate::actor::context::ActorStatus::Stopping;
use crate::actor::message::{Handler, Message};
use crate::actor::metrics::ActorMetrics;
use crate::actor::system::ActorSystem;
use crate::actor::{Actor, ActorId, ActorRefErr, BoxedActorRef, CoreActorRef, LocalActorRef};
use crate::persistent::context::ActorPersistence;
use crate::remote::system::NodeId;
use futures::{Stream, StreamExt};
use std::any::Any;
use tokio::sync::oneshot::Sender;
use valuable::{NamedField, StructDef, Valuable, Value, Visit};

use crate::actor::supervised::Supervised;

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum ActorStatus {
    Starting,
    Started,
    Stopping,
    Stopped,
}

pub struct ActorContext {
    status: ActorStatus,
    persistence: Option<ActorPersistence>,
    boxed_ref: BoxedActorRef,
    boxed_parent_ref: Option<BoxedActorRef>,
    supervised: Option<Supervised>,
    system: Option<ActorSystem>,
    on_actor_stopped: Option<Vec<Sender<()>>>,
}

impl Drop for ActorContext {
    fn drop(&mut self) {
        if let Some(boxed_parent_ref) = &self.boxed_parent_ref {
            let _ = boxed_parent_ref.notify_child_terminated(self.id().clone());
        }

        if let Some(mut supervised) = self.supervised.take() {
            tokio::spawn(async move { supervised.stop_all().await });
        }

        ActorMetrics::incr_actor_stopped(self.boxed_ref.0.actor_type());

        match self.status {
            ActorStatus::Starting => {
                debug!("actor failed to start, context dropped");
            }

            ActorStatus::Started => {
                if self.system.is_some() && self.system().is_terminated() {
                    debug!(
                        "actor (id={}, type={}) has stopped due to system shutdown",
                        &self.id(),
                        self.boxed_ref.actor_type()
                    );
                } else {
                    debug!("actor (id={}) has stopped unexpectedly", self.id());
                }
            }

            ActorStatus::Stopping => {
                if self.system.is_some() && self.system().is_terminated() {
                    trace!(
                        "actor (id={}) has stopped due to system shutdown",
                        &self.id()
                    );
                } else {
                    debug!(
                        "actor (id={}) was stopping but did not complete the stop procedure",
                        self.id()
                    );
                }
            }

            ActorStatus::Stopped => {
                debug!("actor (id={}) stopped, context dropped", self.id());
            }
        }
    }
}

impl ActorContext {
    pub fn new(
        system: Option<ActorSystem>,
        status: ActorStatus,
        boxed_ref: BoxedActorRef,
    ) -> ActorContext {
        ActorContext {
            boxed_ref,
            status,
            system,
            supervised: None,
            persistence: None,
            boxed_parent_ref: None,
            on_actor_stopped: None,
        }
    }

    pub fn id(&self) -> &ActorId {
        &self.boxed_ref.actor_id()
    }

    pub fn stop(&mut self, on_stopped_handler: Option<Sender<()>>) {
        if let Some(sender) = on_stopped_handler {
            self.add_on_stopped_handler(sender);
        }

        self.set_status(Stopping);
    }

    pub fn system(&self) -> &ActorSystem {
        if let Some(system) = &self.system {
            system
        } else {
            unreachable!()
        }
    }

    pub fn system_mut(&mut self) -> &mut ActorSystem {
        if let Some(system) = &mut self.system {
            system
        } else {
            unreachable!()
        }
    }

    pub fn set_system(&mut self, system: ActorSystem) {
        self.system = Some(system);
    }

    pub fn set_status(&mut self, state: ActorStatus) {
        self.status = state
    }

    pub fn get_status(&self) -> &ActorStatus {
        &self.status
    }

    pub fn is_starting(&self) -> bool {
        self.status == ActorStatus::Starting
    }

    pub fn actor_ref<A: Actor>(&self) -> LocalActorRef<A> {
        (&self.boxed_ref.0)
            .as_any()
            .downcast_ref::<LocalActorRef<A>>()
            .expect("actor_ref")
            .clone()
    }

    pub fn boxed_actor_ref(&self) -> BoxedActorRef {
        self.boxed_ref.clone()
    }

    pub fn child_ref<A: Actor>(&self, id: &ActorId) -> Option<LocalActorRef<A>> {
        self.supervised.as_ref().and_then(|s| s.child(id))
    }

    pub fn boxed_child_ref(&self, id: &ActorId) -> Option<BoxedActorRef> {
        if let Some(supervised) = self.supervised.as_ref() {
            supervised.child_boxed(id)
        } else {
            None
        }
    }

    pub fn attach_child_ref(&mut self, actor_ref: BoxedActorRef) {
        let supervised = {
            if self.supervised.is_none() {
                self.supervised = Some(Supervised::new(self.id().to_string()));
            }

            self.supervised.as_mut().unwrap()
        };

        supervised.attach_child_ref(actor_ref);
    }

    pub fn persistence(&self) -> &ActorPersistence {
        self.persistence
            .as_ref()
            .expect("ctx is not setup for persistence")
    }

    pub fn persistence_mut(&mut self) -> &mut ActorPersistence {
        self.persistence
            .as_mut()
            .expect("ctx is not setup for persistence")
    }

    pub fn set_persistence(&mut self, persistence: ActorPersistence) {
        self.persistence = Some(persistence);
    }

    pub fn supervised_mut(&mut self) -> Option<&mut Supervised> {
        self.supervised.as_mut()
    }

    pub async fn spawn<A: Actor>(
        &mut self,
        id: ActorId,
        actor: A,
    ) -> Result<LocalActorRef<A>, ActorRefErr> {
        let supervised = {
            if self.supervised.is_none() {
                self.supervised = Some(Supervised::new(self.id().to_string()));
            }

            self.supervised.as_mut().unwrap()
        };

        let system = self.system.as_ref().unwrap().clone();
        let parent_ref = self.boxed_ref.clone();
        supervised.spawn(id, actor, system, parent_ref).await
    }

    pub fn supervised_count(&self) -> usize {
        self.supervised.as_ref().map_or(0, |s| s.count())
    }

    pub fn with_parent(mut self, boxed_parent_ref: Option<BoxedActorRef>) -> Self {
        self.boxed_parent_ref = boxed_parent_ref;
        self
    }

    pub fn parent<A: Actor>(&self) -> Option<LocalActorRef<A>> {
        if let Some(parent) = &self.boxed_parent_ref.clone() {
            parent.as_actor()
        } else {
            None
        }
    }

    pub fn boxed_parent(&self) -> Option<BoxedActorRef> {
        self.boxed_parent_ref.clone()
    }

    pub fn add_on_stopped_handler(&mut self, event_handler: Sender<()>) {
        if let Some(handlers) = &mut self.on_actor_stopped {
            handlers.push(event_handler);
        } else {
            self.on_actor_stopped = Some(vec![event_handler]);
        }
    }

    pub fn take_on_stopped_handlers(&mut self) -> Option<Vec<Sender<()>>> {
        self.on_actor_stopped.take()
    }
}

pub fn attach_stream<S, T, R, E, A, M>(
    actor_ref: LocalActorRef<A>,
    stream: S,
    options: StreamAttachmentOptions,
    message_converter: T,
) where
    A: Actor + Handler<M>,
    S: 'static + Stream<Item = Result<R, E>> + Send,
    T: 'static + Fn(R) -> Option<M> + Send,
    M: Message,
    S: Unpin,
{
    tokio::spawn(async move {
        let mut reader = stream;
        while let Some(Ok(msg)) = reader.next().await {
            if let Some(message) = message_converter(msg) {
                let _ = actor_ref.notify(message);
            }
        }

        if options.stop_on_stream_end {
            let _ = actor_ref.notify_stop();
        }
    });
}

pub struct StreamAttachmentOptions {
    stop_on_stream_end: bool,
}

impl Default for StreamAttachmentOptions {
    fn default() -> Self {
        Self {
            stop_on_stream_end: true,
        }
    }
}

impl ActorContext {
    pub fn with_persistence(self) -> Self {
        let mut ctx = self;
        let persistence = ctx
            .system()
            .persistence()
            .expect("persistence not configured");

        ctx.persistence = Some(ActorPersistence::new(
            persistence.provider(ctx.boxed_ref.type_id()),
        ));

        ctx
    }
}
