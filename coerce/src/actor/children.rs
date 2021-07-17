use std::collections::HashMap;

use crate::actor::{Actor, ActorId, BoxedActorRef, LocalActorRef};

pub struct Children {
    store: HashMap<ActorId, BoxedActorRef>,
}

impl Children {
    pub fn child<A: Actor>(&self, id: &ActorId) -> Option<LocalActorRef<A>> {
        self.store
            .get(id)
            .map_or(None, |a| a.1.downcast_ref::<LocalActorRef<A>>())
            .map(|a| a.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor::system::ActorSystem;
    use crate::actor::{Actor, IntoActor, LocalActorRef};
    use std::any::Any;

    struct TestActor {}

    impl Actor for TestActor {}

    struct ActorChild {}

    impl Actor for ActorChild {}

    #[tokio::test]
    pub async fn test_actor_ref_type_casting() {
        let mut system = ActorSystem::new();
        let actor_id = "actor".to_string();
        let actor: LocalActorRef<TestActor> = TestActor {}
            .into_actor(Some(actor_id), &system)
            .await
            .expect("create actor");

        let mut actor_box: Box<dyn Any + Send + Sync> = Box::new(actor);

        assert_eq!(
            true,
            actor_box
                .downcast_mut::<LocalActorRef<TestActor>>()
                .is_some()
        );

        assert_eq!(
            false,
            actor_box
                .downcast_mut::<LocalActorRef<ActorChild>>()
                .is_some()
        );
    }
}
