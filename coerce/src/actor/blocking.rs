//! Blocking Actor creation and communication APIs

use crate::actor::lifecycle::Stop;
use crate::actor::message::{ActorMessage, Exec, Handler, Message};
use crate::actor::metrics::ActorMetrics;
use crate::actor::scheduler::{start_actor, ActorType, RegisterActor};
use crate::actor::system::ActorSystem;
use crate::actor::{Actor, ActorRefErr, IntoActorId, LocalActorRef};
use tokio::sync::oneshot;

impl ActorSystem {
    pub fn new_actor_blocking<I: IntoActorId, A: Actor>(
        &self,
        id: I,
        actor: A,
        actor_type: ActorType,
    ) -> Result<LocalActorRef<A>, ActorRefErr> {
        let id = id.into_actor_id();
        let (tx, rx) = oneshot::channel();
        let actor_ref = start_actor(
            actor,
            id.clone(),
            actor_type,
            Some(tx),
            Some(self.clone()),
            None,
            self.system_name().clone(),
        );

        if actor_type.is_tracked() {
            let _ = self.scheduler().send_blocking(RegisterActor {
                id: id.clone(),
                actor_ref: actor_ref.clone(),
            });
        }

        match rx.blocking_recv() {
            Ok(_) => Ok(actor_ref),
            Err(_e) => {
                error!(
                    actor_id = id.as_ref(),
                    actor_type = A::type_name(),
                    "actor not started",
                );
                Err(ActorRefErr::ActorStartFailed)
            }
        }
    }
}

impl<A: Actor> LocalActorRef<A> {
    pub fn send_blocking<Msg: Message>(&self, msg: Msg) -> Result<Msg::Result, ActorRefErr>
    where
        A: Handler<Msg>,
    {
        let message_type = msg.name();
        let actor_type = A::type_name();

        ActorMetrics::incr_messages_sent(A::type_name(), msg.name());

        let (tx, rx) = oneshot::channel();
        match self
            .sender()
            .send(Box::new(ActorMessage::new(msg, Some(tx))))
        {
            Ok(_) => match rx.blocking_recv() {
                Ok(res) => {
                    trace!(
                        "recv result (msg_type={msg_type} actor_type={actor_type})",
                        msg_type = message_type,
                        actor_type = actor_type
                    );

                    Ok(res)
                }
                Err(_e) => Err(ActorRefErr::ResultChannelClosed),
            },
            Err(_e) => Err(ActorRefErr::InvalidRef),
        }
    }

    pub fn stop_blocking(&self) -> Result<(), ActorRefErr> {
        let (tx, rx) = oneshot::channel();
        let res = self.notify(Stop(Some(tx)));
        if res.is_ok() {
            rx.blocking_recv().map_err(|_| ActorRefErr::InvalidRef)
        } else {
            res
        }
    }

    pub fn exec_blocking<F, R>(&self, f: F) -> Result<R, ActorRefErr>
    where
        F: (FnMut(&mut A) -> R) + 'static + Send + Sync,
        R: 'static + Send + Sync,
    {
        self.send_blocking(Exec::new(f))
    }
}
