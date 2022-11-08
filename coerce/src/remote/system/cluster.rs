use crate::remote::actor::message::{ClientWrite, GetNodes, NewClient, RegisterNode, UpdateNodes};
use crate::remote::cluster::node::{RemoteNode, RemoteNodeState};
use crate::remote::net::client::{ClientType, RemoteClientRef};
use crate::remote::net::message::SessionEvent;
use crate::remote::system::{NodeId, RemoteActorSystem};
use std::sync::atomic::Ordering;

impl RemoteActorSystem {
    pub async fn register_node(&self, node: RemoteNode) {
        self.inner
            .registry_ref
            .send(RegisterNode(node))
            .await
            .unwrap()
    }

    pub fn notify_register_node(&self, node: RemoteNode) {
        let _ = self.inner.registry_ref.notify(RegisterNode(node));
    }

    pub async fn get_nodes(&self) -> Vec<RemoteNodeState> {
        self.inner.registry_ref.send(GetNodes).await.unwrap()
    }

    pub async fn update_nodes(&self, nodes: Vec<RemoteNodeState>) {
        self.inner
            .registry_ref
            .send(UpdateNodes(nodes))
            .await
            .unwrap()
    }

    pub async fn notify_node(&self, node_id: NodeId, message: SessionEvent) {
        self.inner
            .clients_ref
            .send(ClientWrite(node_id, message))
            .await
            .unwrap()
    }

    pub fn current_leader(&self) -> Option<NodeId> {
        let n = self.inner.current_leader.load(Ordering::SeqCst);
        if n >= 0 {
            Some(n as NodeId)
        } else {
            None
        }
    }

    pub fn update_leader(&self, new_leader: NodeId) -> Option<NodeId> {
        let n = self
            .inner
            .current_leader
            .swap(new_leader as i64, Ordering::SeqCst);
        if n >= 0 {
            Some(n as NodeId)
        } else {
            None
        }
    }

    pub async fn get_remote_client(&self, addr: String) -> Option<RemoteClientRef> {
        self.client_registry()
            .send(NewClient {
                addr,
                client_type: ClientType::Worker,
                system: self.clone(),
            })
            .await
            .expect("get client from RemoteClientRegistry")
            .map(RemoteClientRef::from)
    }
}
