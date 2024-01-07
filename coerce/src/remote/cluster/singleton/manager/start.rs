use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message};
use crate::actor::{Actor, ActorRefErr, IntoActor, LocalActorRef};
use crate::remote::cluster::singleton::factory::SingletonFactory;
use crate::remote::cluster::singleton::manager::{Manager, State};

pub struct ActorStarted<A: Actor> {
    actor_ref: LocalActorRef<A>,
}

pub struct ActorFailure {
    error: ActorRefErr,
}

impl<A: Actor> Message for ActorStarted<A> {
    type Result = ();
}

impl Message for ActorFailure {
    type Result = ();
}

impl<F: SingletonFactory> Manager<F> {
    pub async fn start_actor(&mut self, ctx: &ActorContext) {
        let state = self.factory.create();
        let sys = self.sys.actor_system().clone();
        let manager_ref = self.actor_ref(ctx);
        let actor_id = self.singleton_actor_id.clone();
        let _ = tokio::spawn(async move {
            let actor = state.into_actor(Some(actor_id), &sys).await;
            match actor {
                Ok(actor_ref) => {
                    let _ = manager_ref.notify(ActorStarted { actor_ref });
                }
                Err(error) => {
                    let _ = manager_ref.notify(ActorFailure { error });
                }
            }
        });
    }
}

#[async_trait]
impl<F: SingletonFactory> Handler<ActorStarted<F::Actor>> for Manager<F> {
    async fn handle(&mut self, message: ActorStarted<F::Actor>, ctx: &mut ActorContext) {
        match &self.state {
            State::Starting { .. } => {
                info!("singleton actor started");
            }
            _ => {
                warn!("Invalid state, expected `Starting`");
            }
        }

        let actor_ref = message.actor_ref;
        self.state = State::Running { actor_ref }

        // TODO: broadcast to all managers that we're running the actor
    }
}

#[async_trait]
impl<F: SingletonFactory> Handler<ActorFailure> for Manager<F> {
    async fn handle(&mut self, message: ActorFailure, ctx: &mut ActorContext) {
        error!("Actor start failed (error={})", message.error);
        // TODO: retry start??
    }
}
