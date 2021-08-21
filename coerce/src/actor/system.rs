use crate::actor::scheduler::{start_actor, ActorScheduler, ActorType, GetActor, RegisterActor};
use crate::actor::{new_actor_id, Actor, ActorId, ActorRefErr, CoreActorRef, LocalActorRef};
use crate::remote::system::RemoteActorSystem;

use crate::persistent::Persistence;
use slog::Logger;
use std::iter::Fuse;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::oneshot::error::RecvError;
use uuid::Uuid;
use sloggers::terminal::{TerminalLoggerBuilder, Destination};
use sloggers::types::{Severity, Format};
use sloggers::Build;

lazy_static! {
    static ref CURRENT_SYSTEM: ActorSystem = ActorSystem::new();
}

#[derive(Clone)]
pub struct ActorSystem {
    system_id: Uuid,
    scheduler: LocalActorRef<ActorScheduler>,
    remote: Option<RemoteActorSystem>,
    persistence: Option<Persistence>,
    is_terminated: Arc<AtomicBool>,
    log: Logger,
}

impl ActorSystem {
    pub fn new() -> ActorSystem {
        ActorSystem::new_with_logger(|_| create_default_logger())
    }

    pub fn new_with_logger<F: Fn(Uuid) -> Logger>(logger_provider: F) -> ActorSystem {
        let system_id = Uuid::new_v4();
        let logger = logger_provider(system_id);
        ActorSystem {
            system_id,
            scheduler: ActorScheduler::new(system_id, logger.clone()),
            remote: None,
            persistence: None,
            is_terminated: Arc::new(AtomicBool::new(false)),
            log: logger,
        }
    }

    pub fn system_id(&self) -> &Uuid {
        &self.system_id
    }

    pub fn scheduler(&self) -> &LocalActorRef<ActorScheduler> {
        &self.scheduler
    }

    pub fn log(&self) -> &Logger {
        &self.log
    }

    pub fn current_system() -> ActorSystem {
        CURRENT_SYSTEM.clone()
    }

    pub fn set_remote(&mut self, remote: RemoteActorSystem) {
        self.remote = Some(remote);
    }

    pub fn remote(&self) -> &RemoteActorSystem {
        self.remote
            .as_ref()
            .expect("this ActorSystem is not setup for remoting")
    }

    pub fn remote_owned(&self) -> RemoteActorSystem {
        self.remote
            .as_ref()
            .map(|s| s.clone())
            .expect("this ActorSystem is not setup for remoting")
    }

    pub fn is_remote(&self) -> bool {
        self.remote.is_some()
    }

    pub async fn new_tracked_actor<A: Actor>(
        &self,
        actor: A,
    ) -> Result<LocalActorRef<A>, ActorRefErr> {
        let id = new_actor_id();
        self.new_actor(id, actor, ActorType::Tracked).await
    }

    pub async fn new_anon_actor<A: Actor>(
        &self,
        actor: A,
    ) -> Result<LocalActorRef<A>, ActorRefErr> {
        let id = new_actor_id();
        self.new_actor(id, actor, ActorType::Anonymous).await
    }

    pub async fn new_actor<A: Actor>(
        &self,
        id: ActorId,
        actor: A,
        actor_type: ActorType,
    ) -> Result<LocalActorRef<A>, ActorRefErr> {
        let actor_type_name = A::type_name();
        let span = tracing::trace_span!(
            "ActorSystem::new_actor",
            actor_type = match actor_type {
                ActorType::Anonymous => "Anonymous",
                _ => "Tracked",
            },
            actor_type_name = actor_type_name,
            actor_id = id.as_str(),
        );

        let _enter = span.enter();

        let (tx, rx) = tokio::sync::oneshot::channel();
        let actor_ref = start_actor(
            actor,
            id.clone(),
            actor_type,
            Some(tx),
            Some(self.clone()),
            None,
            None,
        );

        if actor_type.is_tracked() {
            let _ = self
                .scheduler
                .send(RegisterActor {
                    id: id.clone(),
                    actor_ref: actor_ref.clone(),
                })
                .await;
        }

        match rx.await {
            Ok(_) => Ok(actor_ref),
            Err(e) => {
                error!(
                    &self.log,
                    "actor not started, actor_id={}, type={}",
                    &id,
                    A::type_name()
                );
                Err(ActorRefErr::ActorUnavailable)
            }
        }
    }

    pub fn is_terminated(&self) -> bool {
        self.is_terminated.load(Relaxed)
    }

    pub async fn shutdown(&self) {
        info!(&self.log, "shutting down");

        self.is_terminated.store(true, Relaxed);
        self.scheduler.stop().await;
    }

    pub async fn get_tracked_actor<A: Actor>(&self, id: ActorId) -> Option<LocalActorRef<A>> {
        let actor_type_name = A::type_name();
        let span = tracing::trace_span!(
            "ActorSystem::get_tracked_actor",
            actor_id = id.as_str(),
            actor_type_name
        );
        let _enter = span.enter();

        match self.scheduler.send(GetActor::new(id)).await {
            Ok(a) => a,
            Err(_) => None,
        }
    }

    pub fn set_persistence(&mut self, persistence: Option<Persistence>) {
        self.persistence = persistence;
    }

    pub fn persistence(&self) -> Option<&Persistence> {
        self.persistence.as_ref()
    }
}

fn create_default_logger() -> Logger {
    let mut builder = TerminalLoggerBuilder::new();
    builder.level(Severity::Trace);
    builder.destination(Destination::Stdout);
    builder.format(Format::Compact);
    builder.build().unwrap()
}