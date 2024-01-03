use crate::actor::message::{Handler, Message};
use crate::actor::{Actor, ActorFactory, ActorRefErr};
use crate::remote::cluster::singleton::factory::SingletonFactory;

pub mod factory;
pub mod manager;
pub mod proto;

pub struct Singleton<A: Actor, F: SingletonFactory<Actor = A>> {
    manager: F,
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
