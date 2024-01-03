use crate::actor::Actor;

pub trait SingletonFactory: 'static + Sync + Send {
    type Actor: Actor;

    fn create(&self) -> Self::Actor;
}
