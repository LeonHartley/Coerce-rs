use crate::actor::message::{Handler, Message};
use crate::actor::{Actor, ActorFactory, ActorId, ActorRefErr, IntoActor, IntoActorId, LocalActorRef, ToActorId};
use crate::remote::cluster::node::NodeSelector;
use crate::remote::cluster::singleton::factory::SingletonFactory;
use crate::remote::cluster::singleton::manager::lease::{LeaseAck, RequestLease};
use crate::remote::cluster::singleton::manager::Manager;
use crate::remote::system::builder::RemoteSystemConfigBuilder;
use crate::remote::system::RemoteActorSystem;

pub mod factory;
pub mod manager;
pub mod proto;

pub struct Singleton<A: Actor, F: SingletonFactory<Actor = A>> {
    manager: LocalActorRef<Manager<F>>,
}

pub struct SingletonBuilder<F: SingletonFactory> {
    factory: Option<F>,
    singleton_id: Option<ActorId>,
    manager_id: Option<ActorId>,
    node_selector: NodeSelector,
    system: RemoteActorSystem,
}

impl<F: SingletonFactory> SingletonBuilder<F> {
    pub fn new(system: RemoteActorSystem) -> Self {
        Self {
            system,
            factory: None,
            singleton_id: Some(F::Actor::type_name().into_actor_id()),
            manager_id: Some(Manager::<F>::type_name().into_actor_id()),
            node_selector: NodeSelector::All,
        }
    }

    pub fn factory(mut self, factory: F) -> Self {
        self.factory = Some(factory);
        self
    }

    pub async fn build(mut self) -> Singleton<F::Actor, F> {
        let factory = self.factory.expect("factory");
        let base_manager_id = self.manager_id.expect("manager actor id");
        let manager_actor_id = format!("{}-{}", &base_manager_id, self.system.node_id()).to_actor_id();
        let singleton_actor_id = self.singleton_id.expect("singleton actor id");
        let actor_system = self.system.actor_system().clone();

        let manager = Manager::new(
            self.system,
            factory,
            base_manager_id,
            singleton_actor_id,
            self.node_selector,
        )
        .into_actor(Some(manager_actor_id), &actor_system)
        .await
        .expect("start manager actor");

        Singleton { manager }
    }
}

impl<A: Actor, F: SingletonFactory<Actor = A>> Singleton<A, F> {
    pub async fn send<M: Message>(&self, message: M) -> Result<M::Result, ActorRefErr>
    where
        A: Handler<M>,
    {
        unimplemented!()
    }

    pub async fn notify<M: Message>(&self, message: M) -> Result<(), ActorRefErr>
    where
        A: Handler<M>,
    {
        unimplemented!()
    }
}

pub fn singleton<F: SingletonFactory>(
    builder: &mut RemoteSystemConfigBuilder,
) -> &mut RemoteSystemConfigBuilder {
    let actor_type = F::Actor::type_name();
    builder
        .with_handler::<Manager<F>, RequestLease>(format!(
            "SingletonManager<{}>.RequestLease",
            actor_type
        ))
        .with_handler::<Manager<F>, LeaseAck>(format!("SingletonManager<{}>.LeaseAck", actor_type))
}
