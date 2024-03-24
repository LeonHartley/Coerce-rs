use tokio::sync::oneshot;
use coerce::actor::context::ActorContext;
use coerce::actor::message::{Handler, Message};
use crate::protocol::simple::Error;
use crate::simple::Replicator;
use crate::storage::{Key, Storage, Value};

pub struct Write<K: Key, V: Value> {
    pub key: K,
    pub value: V,
    pub on_completion: Option<oneshot::Sender<Result<(), Error>>>,
}

impl<K: Key, V: Value> Message for Write<K, V> {
    type Result = ();
}

#[async_trait]
impl<S: Storage> Handler<Write<S::Key, S::Value>> for Replicator<S> {
    async fn handle(&mut self, message: Write<S::Key, S::Value>, ctx: &mut ActorContext) {

    }
}
