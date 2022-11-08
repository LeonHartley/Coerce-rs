use coerce::actor::context::ActorContext;

use coerce::actor::{Actor, ActorCreationErr, ActorFactory, ActorRecipe};

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;

#[derive(Clone)]
pub struct ShardedActor {
    id: String,
    actor_started: Arc<AtomicBool>,
    actor_stopped: Arc<AtomicBool>,
    actor_dropped: Arc<AtomicBool>,
}

#[async_trait]
impl Actor for ShardedActor {
    async fn stopped(&mut self, _ctx: &mut ActorContext) {
        self.actor_stopped.store(true, Relaxed);
    }
}

pub struct ShardedActorRecipe {
    id: String,
}

impl ActorRecipe for ShardedActorRecipe {
    fn read_from_bytes(bytes: &Vec<u8>) -> Option<Self> {
        Some(Self {
            id: String::from_utf8(bytes.clone()).unwrap(),
        })
    }

    fn write_to_bytes(&self) -> Option<Vec<u8>> {
        Some(self.id.clone().into_bytes())
    }
}

pub struct ShardedActorFactory {
    state_store: Arc<HashMap<String, ShardedActor>>,
}

impl Clone for ShardedActorFactory {
    fn clone(&self) -> Self {
        Self {
            state_store: self.state_store.clone(),
        }
    }
}

impl ShardedActorFactory {
    pub fn new(actors: Vec<ShardedActor>) -> Self {
        Self {
            state_store: Arc::new(actors.into_iter().map(|a| (a.id.clone(), a)).collect()),
        }
    }

    pub fn assert_actor_started(&self, id: &str) {
        let actor_started = self
            .state_store
            .get(id)
            .unwrap()
            .actor_started
            .load(Relaxed);
        assert!(actor_started)
    }
    pub fn assert_actor_stopped(&self, id: &str) {
        let actor_stopped = self
            .state_store
            .get(id)
            .unwrap()
            .actor_stopped
            .load(Relaxed);
        assert!(actor_stopped)
    }

    pub fn assert_actor_dropped(&self, id: &str) {
        let actor_dropped = self
            .state_store
            .get(id)
            .unwrap()
            .actor_dropped
            .load(Relaxed);
        assert!(actor_dropped)
    }
}

#[async_trait]
impl ActorFactory for ShardedActorFactory {
    type Actor = ShardedActor;
    type Recipe = ShardedActorRecipe;

    async fn create(&self, recipe: ShardedActorRecipe) -> Result<Self::Actor, ActorCreationErr> {
        self.state_store.get(&recipe.id).cloned().ok_or_else(|| {
            ActorCreationErr::InvalidRecipe(format!(
                "no actor with id `{}` in state_store",
                &recipe.id
            ))
        })
    }
}

impl Drop for ShardedActor {
    fn drop(&mut self) {
        self.actor_dropped.store(true, Relaxed);
    }
}
