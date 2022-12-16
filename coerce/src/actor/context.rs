use crate::actor::message::{Handler, Message};
use crate::actor::metrics::ActorMetrics;
use crate::actor::system::ActorSystem;
use crate::actor::{
    Actor, ActorId, ActorPath, ActorRefErr, ActorTags, BoxedActorRef, CoreActorRef, IntoActorPath,
    LocalActorRef,
};
use futures::{Stream, StreamExt};
use std::any::Any;
use std::time::Instant;
use tokio::sync::oneshot::Sender;
use valuable::{Fields, NamedField, NamedValues, StructDef, Structable, Valuable, Value, Visit};

use crate::actor::supervised::Supervised;

#[cfg(feature = "persistence")]
use crate::persistent::context::ActorPersistence;

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum ActorStatus {
    Starting,
    Started,
    Stopping,
    Stopped,
}
//
// struct LastMessage {
//     received_at: DateTime<Utc>,
// }

pub struct ActorContext {
    context_id: u64,
    status: ActorStatus,
    boxed_ref: BoxedActorRef,
    boxed_parent_ref: Option<BoxedActorRef>,
    supervised: Option<Supervised>,
    system: Option<ActorSystem>,
    on_actor_stopped: Option<Vec<Sender<()>>>,
    tags: Option<ActorTags>,
    full_path: ActorPath,

    #[cfg(feature = "persistence")]
    persistence: Option<ActorPersistence>,
}

pub struct LogContext {
    pub actor_path: ActorPath,
    pub actor_type: &'static str,
}

impl ActorContext {
    pub fn new(
        system: Option<ActorSystem>,
        status: ActorStatus,
        boxed_ref: BoxedActorRef,
    ) -> ActorContext {
        let context_id = system.as_ref().map_or_else(|| 0, |s| s.next_context_id());
        let full_path =
            format!("{}/{}", boxed_ref.actor_path(), boxed_ref.actor_id()).into_actor_path();

        ActorContext {
            boxed_ref,
            status,
            system,
            context_id,
            full_path,
            supervised: None,
            boxed_parent_ref: None,
            on_actor_stopped: None,
            tags: None,
            // last_message_timestamp: None,
            #[cfg(feature = "persistence")]
            persistence: None,
        }
    }

    pub fn id(&self) -> &ActorId {
        self.boxed_ref.actor_id()
    }

    pub fn path(&self) -> &ActorPath {
        self.boxed_ref.actor_path()
    }

    pub fn full_path(&self) -> &ActorPath {
        &self.full_path
    }

    pub fn ctx_id(&self) -> u64 {
        self.context_id
    }

    pub fn set_tags(&mut self, tags: impl Into<ActorTags>) {
        let tags = tags.into();
        self.tags = Some(tags);
    }

    pub fn tags(&self) -> ActorTags {
        self.tags.as_ref().map_or(ActorTags::None, |t| t.clone())
    }

    pub fn stop(&mut self, on_stopped_handler: Option<Sender<()>>) {
        if let Some(sender) = on_stopped_handler {
            self.add_on_stopped_handler(sender);
        }

        self.set_status(ActorStatus::Stopping);
    }

    pub fn system(&self) -> &ActorSystem {
        if let Some(system) = &self.system {
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
                self.supervised = Some(Supervised::new(self.id().clone(), self.path().clone()));
            }

            self.supervised.as_mut().unwrap()
        };

        supervised.attach_child_ref(actor_ref);
    }

    #[cfg(feature = "persistence")]
    pub fn persistence(&self) -> &ActorPersistence {
        self.persistence
            .as_ref()
            .expect("ctx is not setup for persistence")
    }

    #[cfg(feature = "persistence")]
    pub fn persistence_mut(&mut self) -> &mut ActorPersistence {
        self.persistence
            .as_mut()
            .expect("ctx is not setup for persistence")
    }

    #[cfg(feature = "persistence")]
    pub fn set_persistence(&mut self, persistence: ActorPersistence) {
        self.persistence = Some(persistence);
    }

    pub fn supervised_mut(&mut self) -> Option<&mut Supervised> {
        self.supervised.as_mut()
    }

    pub fn supervised(&self) -> Option<&Supervised> {
        self.supervised.as_ref()
    }

    pub async fn spawn<A: Actor>(
        &mut self,
        id: ActorId,
        actor: A,
    ) -> Result<LocalActorRef<A>, ActorRefErr> {
        let supervised = {
            if self.supervised.is_none() {
                self.supervised =
                    Some(Supervised::new(self.id().clone(), self.full_path().clone()));
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

    pub fn log(&self) -> LogContext {
        LogContext {
            actor_path: self.full_path.clone(),
            actor_type: self.boxed_ref.actor_type(),
        }
    }
}

static LOG_CONTEXT_FIELDS: &[NamedField<'static>] =
    &[NamedField::new("actor_path"), NamedField::new("actor_type")];

impl Structable for LogContext {
    fn definition(&self) -> StructDef<'_> {
        StructDef::new_static("Context", Fields::Named(LOG_CONTEXT_FIELDS))
    }
}

impl Valuable for LogContext {
    fn as_value(&self) -> Value<'_> {
        Value::Structable(self)
    }

    fn visit(&self, v: &mut dyn Visit) {
        v.visit_named_fields(&NamedValues::new(
            LOG_CONTEXT_FIELDS,
            &[
                Value::String(self.actor_path.as_ref()),
                Value::String(self.actor_type),
            ],
        ));
    }
}

impl Drop for ActorContext {
    fn drop(&mut self) {
        let parent_ref = self.boxed_parent_ref.take();

        if let Some(mut supervised) = self.supervised.take() {
            let parent_ref = parent_ref.clone();
            let boxed_ref = self.boxed_ref.clone();
            let system = self.system.clone();
            let status = self.status.clone();

            tokio::spawn(async move {
                supervised.stop_all().await;

                on_context_dropped(&boxed_ref, &parent_ref, &status, &system);
            });
        } else {
            on_context_dropped(&self.boxed_ref, &parent_ref, &self.status, &self.system);
        }
    }
}

fn on_context_dropped(
    actor: &BoxedActorRef,
    parent_ref: &Option<BoxedActorRef>,
    status: &ActorStatus,
    system: &Option<ActorSystem>,
) {
    ActorMetrics::incr_actor_stopped(actor.actor_type());

    let actor_id = actor.0.actor_id();
    let actor_type = actor.0.actor_type();
    let system_terminated = system.as_ref().map(|s| s.is_terminated());

    match status {
        ActorStatus::Starting => {
            debug!("actor failed to start, context dropped");
        }

        ActorStatus::Started => {
            if let Some(true) = system_terminated {
                debug!(
                    "actor (id={}, type={}) has stopped due to system shutdown",
                    actor_id, actor_type
                );
            } else {
                debug!("actor (id={}) has stopped unexpectedly", actor.0.actor_id());
            }
        }

        ActorStatus::Stopping => {
            if let Some(true) = system_terminated {
                trace!("actor (id={}) has stopped due to system shutdown", actor_id,);
            } else {
                debug!(
                    "actor (id={}) was stopping but did not complete the stop procedure",
                    actor_id,
                );
            }
        }

        ActorStatus::Stopped => {
            debug!("actor (id={}) stopped, context dropped", actor_id);
        }
    }

    if let Some(boxed_parent_ref) = parent_ref {
        let _ = boxed_parent_ref.notify_child_terminated(actor_id.clone());
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
    #[cfg(feature = "persistence")]
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
