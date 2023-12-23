use crate::actor::context::ActorContext;
use crate::actor::message::Handler;
use crate::actor::{Actor, ActorFactory, LocalActorRef};
use crate::remote::cluster::singleton::factory::SingletonFactory;
use crate::remote::stream::pubsub::{PubSub, Receive, Subscription};
use crate::remote::stream::system::{ClusterEvent, SystemEvent, SystemTopic};
use crate::remote::system::{NodeId, RemoteActorSystem};

pub struct Manager<F: SingletonFactory> {
    system_event_subscription: Option<Subscription>,
    state: State<F::Actor>,
    factory: F,
}

impl<A: Actor> Default for State<A> {
    fn default() -> Self {
        Self::Idle
    }
}

pub enum State<A: Actor> {
    Idle,
    Running { actor_ref: LocalActorRef<A> },
    Stopping { actor_ref: LocalActorRef<A> },
}

impl<A: Actor> State<A> {
    pub fn is_running(&self) -> bool {
        matches!(self, Self::Running { .. })
    }
}

impl<F: SingletonFactory> Manager<F> {
    pub fn start_actor(&mut self) {

    }

    pub fn on_leader_changed(&mut self, new_leader_id: &NodeId, sys: &RemoteActorSystem) {
        if new_leader_id == sys.node_id() {
            if !self.state.is_running() {
                self.start_actor();
            }
        }
    }
}

#[async_trait]
impl<F: ActorFactory> Actor for Manager<F> {
    async fn started(&mut self, ctx: &mut ActorContext) {
        self.system_event_subscription = Some(
            PubSub::subscribe::<Self, SystemTopic>(SystemTopic, ctx)
                .await
                .unwrap(),
        );
    }
}

#[async_trait]
impl<F: ActorFactory> Handler<Receive<SystemTopic>> for Manager<F> {
    async fn handle(&mut self, message: Receive<SystemTopic>, ctx: &mut ActorContext) {
        match message.0.as_ref() {
            SystemEvent::Cluster(e) => match e {
                ClusterEvent::LeaderChanged(leader) => {
                    let sys = ctx.system().remote();
                    self.on_leader_changed(leader, &sys).await
                }
                _ => {}
            },
        }
    }
}
