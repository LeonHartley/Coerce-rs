use crate::actor::context::{ActorContext, ActorStatus};
use crate::actor::message::Message;
use crate::actor::system::ActorSystem;
use crate::actor::{Actor, BoxedActorRef};

use crate::persistent::journal::snapshot::Snapshot;
use crate::persistent::journal::types::JournalTypes;
use crate::persistent::journal::{PersistErr, RecoveredPayload};

#[async_trait]
pub trait PersistentActor: 'static + Sized + Send + Sync {
    fn persistence_key(&self, ctx: &ActorContext) -> String {
        ctx.id().to_string()
    }

    fn configure(types: &mut JournalTypes<Self>);

    async fn pre_recovery(&mut self, _ctx: &mut ActorContext) {}

    async fn post_recovery(&mut self, _ctx: &mut ActorContext) {}

    async fn stopped(&mut self, _ctx: &mut ActorContext) {}

    async fn persist<M: Message>(
        &self,
        message: &M,
        ctx: &mut ActorContext,
    ) -> Result<(), PersistErr>
    where
        Self: Recover<M>,
    {
        ctx.persistence_mut()
            .journal_mut::<Self>()
            .persist_message(message)
            .await
    }

    async fn snapshot<S: Snapshot>(
        &self,
        snapshot: S,
        ctx: &mut ActorContext,
    ) -> Result<(), PersistErr>
    where
        Self: RecoverSnapshot<S>,
    {
        ctx.persistence_mut()
            .journal_mut::<Self>()
            .persist_snapshot(snapshot)
            .await
    }
}

#[async_trait]
impl<A: 'static + PersistentActor + Send + Sync> Actor for A
where
    A: Sized,
{
    fn new_context(
        system: Option<ActorSystem>,
        status: ActorStatus,
        boxed_ref: BoxedActorRef,
    ) -> ActorContext
    where
        Self: Sized,
    {
        ActorContext::new(system, status, boxed_ref).with_persistence()
    }

    async fn started(&mut self, ctx: &mut ActorContext) {
        trace!("persistent actor starting, loading journal");
        self.pre_recovery(ctx).await;

        let persistence_key = self.persistence_key(ctx);
        let (snapshot, messages) = load_journal::<A>(persistence_key.clone(), ctx).await;

        trace!(
            "persistent actor ({}) recovered {} snapshot(s) and {} message(s)",
            &persistence_key,
            if snapshot.is_some() { 1 } else { 0 },
            messages.as_ref().map_or(0, |m| m.len()),
        );

        if let Some(snapshot) = snapshot {
            snapshot.recover(self, ctx).await;
        }

        if let Some(messages) = messages {
            for message in messages {
                message.recover(self, ctx).await;
            }
        }

        self.post_recovery(ctx).await;
    }

    async fn stopped(&mut self, ctx: &mut ActorContext) {
        trace!("persistent actor stopped");

        self.stopped(ctx).await
    }
}

async fn load_journal<A: PersistentActor>(
    persistence_key: String,
    ctx: &mut ActorContext,
) -> (
    Option<RecoveredPayload<A>>,
    Option<Vec<RecoveredPayload<A>>>,
) {
    let journal = ctx
        .persistence_mut()
        .init_journal::<A>(persistence_key)
        .await;
    (
        journal.recover_snapshot().await,
        journal.recover_messages().await,
    )
}

#[async_trait]
pub trait Recover<M: Message> {
    async fn recover(&mut self, message: M, ctx: &mut ActorContext);
}

#[async_trait]
pub trait RecoverSnapshot<S: Snapshot> {
    async fn recover(&mut self, snapshot: S, ctx: &mut ActorContext);
}
