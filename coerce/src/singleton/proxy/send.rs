use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message};
use crate::actor::{Actor, ActorRef, ActorRefErr};
use crate::singleton::factory::SingletonFactory;
use crate::singleton::proxy::{Proxy, ProxyState};
use tokio::sync::oneshot::Sender;

pub struct Deliver<M: Message> {
    message: Option<M>,
    result_channel: Option<Sender<Result<M::Result, ActorRefErr>>>,
}

impl<M: Message> Deliver<M> {
    pub fn new(message: M, result_channel: Option<Sender<Result<M::Result, ActorRefErr>>>) -> Self {
        Self {
            message: Some(message),
            result_channel,
        }
    }
}

impl<M: Message> Deliver<M> {
    pub fn deliver<A>(&mut self, actor: ActorRef<A>)
    where
        A: Handler<M>,
    {
        let message = self.message.take().unwrap();
        let result_channel = self.result_channel.take().unwrap();
        tokio::spawn(async move {
            debug!(msg_type = M::type_name(), "delivering message to singleton");

            let res = actor.send(message).await;
            result_channel.send(res)
        });
    }
}

pub trait Buffered<A: Actor>: 'static + Sync + Send {
    fn send(&mut self, actor_ref: ActorRef<A>);
}

impl<A: Actor, M: Message> Buffered<A> for Deliver<M>
where
    A: Handler<M>,
{
    fn send(&mut self, actor_ref: ActorRef<A>) {
        self.deliver(actor_ref)
    }
}

impl<M: Message> Message for Deliver<M> {
    type Result = ();
}

#[async_trait]
impl<A: Actor, M: Message> Handler<Deliver<M>> for Proxy<A>
where
    A: Handler<M>,
{
    async fn handle(&mut self, mut message: Deliver<M>, ctx: &mut ActorContext) {
        match &mut self.state {
            ProxyState::Buffered { request_queue } => {
                request_queue.push_back(Box::new(message));
                debug!(
                    msg_type = M::type_name(),
                    buffered_msgs = request_queue.len(),
                    "singleton proxy buffered message",
                );
            }

            ProxyState::Active { actor_ref } => {
                message.deliver(actor_ref.clone());
                debug!(msg_type = M::type_name(), "singleton proxy sent message");
            }
        }
    }
}
