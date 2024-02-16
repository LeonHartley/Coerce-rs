use crate::actor::message::{Handler, Message};
use crate::actor::{
    Actor, ActorFactory, ActorId, ActorRefErr, IntoActor, IntoActorId, LocalActorRef, ToActorId,
};
use crate::remote::cluster::node::NodeSelector;
use crate::remote::system::builder::RemoteSystemConfigBuilder;
use crate::remote::system::RemoteActorSystem;
use crate::singleton::factory::SingletonFactory;
use crate::singleton::manager::lease::{LeaseAck, RequestLease};
use crate::singleton::manager::{Manager, SingletonStarted, SingletonStopped};
use crate::singleton::proxy::send::Deliver;
use crate::singleton::proxy::Proxy;
use tokio::sync::oneshot;

pub mod factory;
pub mod manager;
pub mod proto;
pub mod proxy;

pub struct Singleton<A: Actor, F: SingletonFactory<Actor = A>> {
    manager: LocalActorRef<Manager<F>>,
    proxy: LocalActorRef<Proxy<A>>,
}

pub struct SingletonBuilder<F: SingletonFactory> {
    factory: Option<F>,
    singleton_id: Option<ActorId>,
    manager_id: Option<ActorId>,
    proxy_id: Option<ActorId>,
    node_selector: NodeSelector,
    system: RemoteActorSystem,
}

impl<F: SingletonFactory> SingletonBuilder<F> {
    pub fn new(system: RemoteActorSystem) -> Self {
        Self {
            system,
            factory: None,
            singleton_id: Some(F::Actor::type_name().into_actor_id()),
            manager_id: Some(
                format!("singleton-manager<{}>", F::Actor::type_name()).into_actor_id(),
            ),
            proxy_id: Some(format!("singleton-proxy<{}>", F::Actor::type_name()).into_actor_id()),
            node_selector: NodeSelector::All,
        }
    }

    pub fn factory(mut self, factory: F) -> Self {
        self.factory = Some(factory);
        self
    }

    pub async fn build(mut self) -> Singleton<F::Actor, F> {
        let factory = self.factory.expect("factory");

        let node_id = self.system.node_id();
        let base_manager_id = self.manager_id.expect("manager actor id");
        let base_proxy_id = self.proxy_id.expect("proxy actor id");

        let manager_actor_id = format!("{}-{}", &base_manager_id, node_id).to_actor_id();
        let proxy_actor_id = format!("{}-{}", &base_proxy_id, node_id).to_actor_id();

        let singleton_actor_id = self.singleton_id.expect("singleton actor id");
        let actor_system = self.system.actor_system().clone();

        let proxy = Proxy::<F::Actor>::new()
            .into_actor(Some(proxy_actor_id), &actor_system)
            .await
            .expect("start proxy actor");

        let manager = Manager::new(
            self.system,
            factory,
            base_manager_id,
            singleton_actor_id,
            self.node_selector,
            proxy.clone(),
        )
        .into_actor(Some(manager_actor_id), &actor_system)
        .await
        .expect("start manager actor");

        Singleton { manager, proxy }
    }
}

impl<A: Actor, F: SingletonFactory<Actor = A>> Singleton<A, F> {
    pub async fn send<M: Message>(&self, message: M) -> Result<M::Result, ActorRefErr>
    where
        A: Handler<M>,
    {
        let (tx, rx) = oneshot::channel();
        if let Err(e) = self.proxy.notify(Deliver::new(message, Some(tx))) {
            return Err(e);
        }

        rx.await.unwrap()
    }

    pub async fn notify<M: Message>(&self, message: M) -> Result<(), ActorRefErr>
    where
        A: Handler<M>,
    {
        self.proxy.notify(Deliver::new(message, None))
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
        .with_handler::<Manager<F>, SingletonStarted>(format!(
            "SingletonManager<{}>.SingletonStarted",
            actor_type
        ))
        .with_handler::<Manager<F>, SingletonStopped>(format!(
            "SingletonManager<{}>.SingletonStopped",
            actor_type
        ))
}

impl<A: Actor, F: SingletonFactory<Actor = A>> Clone for Singleton<A, F> {
    fn clone(&self) -> Self {
        Self {
            manager: self.manager.clone(),
            proxy: self.proxy.clone(),
        }
    }
}
