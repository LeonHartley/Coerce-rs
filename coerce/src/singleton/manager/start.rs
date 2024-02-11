use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message};
use crate::actor::{Actor, ActorRefErr, IntoActor, LocalActorRef, PipeTo};
use crate::singleton::factory::SingletonFactory;
use crate::singleton::manager::{Manager, SingletonStarted, State};
use crate::singleton::proxy;

pub enum ActorStartResult<A: Actor> {
    Started(LocalActorRef<A>),
    Failed(ActorRefErr),
}

impl<A: Actor> Message for ActorStartResult<A> {
    type Result = ();
}

impl<F: SingletonFactory> Manager<F> {
    pub async fn start_actor(&mut self, ctx: &ActorContext) {
        let state = self.factory.create();
        let sys = self.sys.actor_system().clone();
        let manager_ref = self.actor_ref(ctx);
        let actor_id = self.singleton_actor_id.clone();

        async move {
            match state.into_actor(Some(actor_id), &sys).await {
                Ok(actor_ref) => ActorStartResult::Started(actor_ref),
                Err(e) => ActorStartResult::Failed(e),
            }
        }
        .pipe_to(manager_ref.into());
    }
}

#[async_trait]
impl<F: SingletonFactory> Handler<ActorStartResult<F::Actor>> for Manager<F> {
    async fn handle(&mut self, message: ActorStartResult<F::Actor>, ctx: &mut ActorContext) {
        match message {
            ActorStartResult::Started(actor_ref) => {
                match &self.state {
                    State::Starting { .. } => {
                        debug!("singleton actor started");
                    }
                    _ => {
                        warn!("Invalid state, expected `Starting`");
                    }
                }

                self.state = State::Running {
                    actor_ref: actor_ref.clone(),
                };

                let _ = self
                    .proxy
                    .notify(proxy::SingletonStarted::new(actor_ref.into()));

                self.notify_managers(
                    SingletonStarted {
                        source_node_id: self.node_id,
                    },
                    ctx,
                )
                .await;
            }
            ActorStartResult::Failed(e) => {
                error!(error = format!("{}", e), "singleton actor failed to start")
            }
        }
    }
}
