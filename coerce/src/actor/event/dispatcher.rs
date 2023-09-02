
pub struct Dispatcher {
    subscribers: Vec<Receiver<Arc<ActorEvent>>>
}

#[async_trait]
impl Actor for Dispatcher {

}