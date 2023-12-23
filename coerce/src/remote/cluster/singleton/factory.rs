use crate::actor::Actor;

pub trait SingletonFactory {
    type Actor: Actor;

    fn create(&self) -> Self::Actor;
}

