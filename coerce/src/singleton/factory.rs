use crate::actor::Actor;

pub trait SingletonFactory: 'static + Sync + Send {
    type Actor: Actor;

    fn create(&self) -> Self::Actor;
}

impl<A: Actor, F> SingletonFactory for F
where
    F: Fn() -> A + 'static + Sync + Send,
{
    type Actor = A;

    fn create(&self) -> A {
        self()
    }
}
