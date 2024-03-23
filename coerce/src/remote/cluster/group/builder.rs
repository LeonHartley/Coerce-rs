use crate::actor::{Actor, ActorRefErr, LocalActorRef, Receiver};
use crate::remote::cluster::group::{
    ActorIdProvider, DefaultActorIdProvider, NodeGroup, NodeGroupEvent,
};
use crate::remote::cluster::node::NodeSelector;
use crate::remote::system::RemoteActorSystem;

pub struct NodeGroupBuilder<A: Actor> {
    group_name: Option<String>,
    actor_id_provider: Option<Box<dyn ActorIdProvider>>,
    selector: Option<NodeSelector>,
    receivers: Option<Vec<Receiver<NodeGroupEvent<A>>>>,
}

impl<A: Actor> NodeGroupBuilder<A> {
    pub fn new() -> NodeGroupBuilder<A> {
        Self {
            group_name: None,
            actor_id_provider: None,
            selector: None,
            receivers: None,
        }
    }

    pub fn group_name(&mut self, group_name: impl ToString) -> &mut Self {
        self.group_name = Some(group_name.to_string());
        self
    }

    pub fn actor_id_provider(&mut self, actor_id_provider: impl ActorIdProvider) -> &mut Self {
        self.actor_id_provider = Some(Box::new(actor_id_provider));
        self
    }

    pub fn node_selector(&mut self, selector: NodeSelector) -> &mut Self {
        self.selector = Some(selector);
        self
    }

    pub fn receiver(&mut self, receiver: Receiver<NodeGroupEvent<A>>) -> &mut Self {
        if let Some(receivers) = &mut self.receivers {
            receivers.push(receiver);
        } else {
            self.receivers = Some(vec![receiver]);
        }

        self
    }

    pub async fn build(
        &mut self,
        sys: &RemoteActorSystem,
    ) -> Result<LocalActorRef<NodeGroup<A>>, ActorRefErr> {
        let group_name = self
            .group_name
            .take()
            .unwrap_or_else(|| format!("{}-group", A::type_name()));

        let actor_id_provider = self
            .actor_id_provider
            .take()
            .unwrap_or_else(|| Box::new(DefaultActorIdProvider));

        let selector = self.selector.take().unwrap_or_else(|| NodeSelector::All);
        let receivers = self.receivers.take().unwrap_or_else(|| vec![]);

        NodeGroup::new(group_name, actor_id_provider, selector, receivers, &sys).await
    }
}
