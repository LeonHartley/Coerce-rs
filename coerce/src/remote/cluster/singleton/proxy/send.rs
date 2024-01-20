use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message};
use crate::actor::{Actor, ActorRef, ActorRefErr};
use crate::remote::cluster::singleton::factory::SingletonFactory;
use crate::remote::cluster::singleton::proxy::{Proxy, ProxyState};
use tokio::sync::oneshot::Sender;

pub struct Send<M: Message> {
    message: Option<M>,
    result_channel: Option<Sender<M::Result>>,
}

impl<M: Message> Send<M> {
    pub fn deliver<A>(&mut self, actor: ActorRef<A>)
    where
        A: Handler<M>,
    {
        let message = self.message.take().unwrap();
        let result_channel = self.result_channel.take().unwrap();
        tokio::spawn(async move {
            let res = actor.send(message).await;
            match res {
                Ok(r) => {
                    let _ = result_channel.send(r);
                }
                Err(_) => {}
            }
        });
    }
}

pub trait Buffered<A: Actor>: 'static + Sync + std::marker::Send {
    fn send(&mut self, actor_ref: ActorRef<A>);
}

impl<A: Actor, M: Message> Buffered<A> for Send<M>
where
    A: Handler<M>,
{
    fn send(&mut self, actor_ref: ActorRef<A>) {
        self.deliver(actor_ref)
    }
}

impl<M: Message> Message for Send<M> {
    type Result = ();
}

#[async_trait]
impl<A: Actor, M: Message> Handler<Send<M>> for Proxy<A>
where
    A: Handler<M>,
{
    async fn handle(&mut self, mut message: Send<M>, ctx: &mut ActorContext) {
        match &mut self.state {
            ProxyState::Buffered { request_queue } => {
                request_queue.push_back(Box::new(message));
                debug!(
                    "singleton proxy buffered message (msg_type={}, total_buffered={})",
                    M::type_name(),
                    request_queue.len()
                );
            }

            ProxyState::Active { actor_ref } => {
                message.deliver(actor_ref.clone());
                debug!("singleton proxy sent message (msg_type={})", M::type_name());
            }
        }
    }
}
