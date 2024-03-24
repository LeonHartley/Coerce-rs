use crate::actor::watch::ActorTerminated;
use crate::actor::{ActorId, Receiver};
use std::collections::HashMap;

#[derive(Default)]
pub struct Watchers {
    watchers: HashMap<ActorId, Receiver<ActorTerminated>>,
}

impl Watchers {
    pub fn iter(&self) -> impl Iterator<Item = &Receiver<ActorTerminated>> {
        self.watchers.values()
    }

    pub fn add_watcher(&mut self, watcher: Receiver<ActorTerminated>) {
        self.watchers.insert(watcher.actor_id().to_owned(), watcher);
    }

    pub fn remove_watcher(&mut self, actor_id: ActorId) {
        self.watchers.remove(&actor_id);
    }
}
