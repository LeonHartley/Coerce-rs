use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message};
use crate::actor::{Actor, ActorRef, ToActorId};
use crate::singleton::proxy::send::Buffered;
use std::collections::VecDeque;

pub mod send;

pub enum ProxyState<A: Actor> {
    Buffered {
        request_queue: VecDeque<Box<dyn Buffered<A>>>,
    },

    Active {
        actor_ref: ActorRef<A>,
    },
}

pub struct Proxy<A: Actor> {
    state: ProxyState<A>,
}

impl<A: Actor> Proxy<A> {
    pub fn new() -> Self {
        Self {
            state: ProxyState::Buffered {
                request_queue: VecDeque::new(),
            },
        }
    }
}

impl<A: Actor> ProxyState<A> {
    pub fn is_active(&self) -> bool {
        matches!(&self, &ProxyState::Active { .. })
    }
}

#[async_trait]
impl<A: Actor> Actor for Proxy<A> {}

pub struct SingletonStarted<A: Actor> {
    actor_ref: ActorRef<A>,
}

impl<A: Actor> SingletonStarted<A> {
    pub fn new(actor_ref: ActorRef<A>) -> Self {
        Self { actor_ref }
    }
}

pub struct SingletonStopping;

impl<A: Actor> Message for SingletonStarted<A> {
    type Result = ();
}

impl Message for SingletonStopping {
    type Result = ();
}

#[async_trait]
impl<A: Actor> Handler<SingletonStarted<A>> for Proxy<A> {
    async fn handle(&mut self, message: SingletonStarted<A>, ctx: &mut ActorContext) {
        let actor_ref = message.actor_ref;

        match &mut self.state {
            ProxyState::Buffered { request_queue } => {
                if request_queue.len() > 0 {
                    debug!(
                        buffered_msgs = request_queue.len(),
                        actor_ref = format!("{}", &actor_ref),
                        "emitting buffered messages",
                    );

                    while let Some(mut buffered) = request_queue.pop_front() {
                        buffered.send(actor_ref.clone());
                    }
                }
            }
            _ => {}
        }

        debug!(
            singleton_actor = format!("{}", actor_ref),
            "singleton proxy active - singleton started"
        );
        self.state = ProxyState::Active { actor_ref };
    }
}

#[async_trait]
impl<A: Actor> Handler<SingletonStopping> for Proxy<A> {
    async fn handle(&mut self, _: SingletonStopping, ctx: &mut ActorContext) {
        debug!("singleton actor stopped, buffering messages");

        if self.state.is_active() {
            self.state = ProxyState::Buffered {
                request_queue: VecDeque::new(),
            }
        }
    }
}
