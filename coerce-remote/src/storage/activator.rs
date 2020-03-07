use crate::storage::state::{ActorState, ActorStore};
use coerce_rt::actor::{Actor, ActorId};
use std::convert::TryFrom;

pub struct ActorActivator {
    store: Box<dyn ActorStore + Sync + Send>,
}

impl ActorActivator {
    pub fn new(store: Box<dyn ActorStore + Sync + Send>) -> ActorActivator {
        ActorActivator { store }
    }
}

impl ActorActivator {
    pub async fn activate<A: Actor>(&mut self, id: ActorId) -> Option<A>
    where
        A: TryFrom<ActorState>,
    {
        let state = self.store.get(id).await;
        if let Ok(Some(state)) = state {
            match A::try_from(state) {
                Ok(actor) => Some(actor),
                Err(_) => None,
            }
        } else {
            None
        }
    }
}

impl Clone for ActorActivator {
    fn clone(&self) -> Self {
        ActorActivator {
            store: self.store.clone(),
        }
    }
}
