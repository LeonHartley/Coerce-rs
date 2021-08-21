use crate::actor::context::ActorContext;
use crate::actor::message::Handler;
use crate::remote::actor::message::{
    ClientWrite, DeregisterClient, GetActorNode, GetNodes, PopRequest, PushRequest, RegisterActor,
    RegisterClient, RegisterNode, RegisterNodes, SetRemote,
};
use crate::remote::actor::{
    RemoteClientRegistry, RemoteHandler, RemoteRegistry, RemoteRequest, RemoteResponse,
};
use crate::remote::cluster::node::RemoteNode;
use crate::remote::net::client::{ClientType, RemoteClient, RemoteClientStream};
use crate::remote::system::RemoteActorSystem;

use std::collections::HashMap;

use crate::remote::net::message::SessionEvent;
use crate::remote::net::proto::protocol::{ActorAddress, FindActor};
use std::str::FromStr;

use crate::actor::ActorId;
use crate::remote::stream::pubsub::{PubSub, StreamEvent};
use crate::remote::stream::system::{ClusterEvent, SystemEvent, SystemTopic};
use crate::remote::tracing::extract_trace_identifier;

use protobuf::Message;
use uuid::Uuid;

#[async_trait]
impl Handler<SetRemote> for RemoteRegistry {
    async fn handle(&mut self, message: SetRemote, ctx: &mut ActorContext) {
        let sys = message.0;
        ctx.set_system(sys.actor_system().clone());
        self.system = Some(sys);

        let subscription = PubSub::subscribe::<Self, SystemTopic>(SystemTopic, ctx).await;
        if let Ok(subscription) = subscription {
            trace!(ctx.log(), "subscribed to system event");
            self.system_event_subscription = Some(subscription);
        }
    }
}

#[async_trait]
impl Handler<GetNodes> for RemoteRegistry {
    async fn handle(&mut self, _message: GetNodes, _ctx: &mut ActorContext) -> Vec<RemoteNode> {
        self.nodes.get_all()
    }
}

#[async_trait]
impl Handler<PushRequest> for RemoteHandler {
    async fn handle(&mut self, message: PushRequest, _ctx: &mut ActorContext) {
        self.requests.insert(message.0, message.1);
    }
}

#[async_trait]
impl Handler<PopRequest> for RemoteHandler {
    async fn handle(
        &mut self,
        message: PopRequest,
        _ctx: &mut ActorContext,
    ) -> Option<RemoteRequest> {
        self.requests.remove(&message.0)
    }
}

#[async_trait]
impl<T: RemoteClientStream> Handler<RegisterClient<T>> for RemoteClientRegistry
where
    T: 'static + Sync + Send,
{
    async fn handle(&mut self, message: RegisterClient<T>, ctx: &mut ActorContext) {
        self.add_client(message.0, message.1);

        trace!(ctx.log(), "client {} registered", message.0);
    }
}

#[async_trait]
impl Handler<RegisterNodes> for RemoteRegistry {
    async fn handle(&mut self, message: RegisterNodes, ctx: &mut ActorContext) {
        let remote_ctx = self.system.as_ref().unwrap().clone();
        let nodes = message.0;

        let unregistered_nodes = nodes
            .iter()
            .filter(|node| node.id != remote_ctx.node_id() && !self.nodes.is_registered(node.id))
            .map(|node| node.clone())
            .collect();

        trace!(ctx.log(), "registering new nodes {:?}", &unregistered_nodes);
        let current_nodes = self.nodes.get_all();

        let clients = connect_all(unregistered_nodes, current_nodes, &remote_ctx).await;
        for (_node_id, client) in clients {
            if let Some(client) = client {
                remote_ctx.register_client(client.node_id, client).await;
            }
        }

        for node in nodes {
            let sys = remote_ctx.clone();
            let node_id = node.id;
            tokio::spawn(async move {
                let sys = sys;
                PubSub::publish_locally(
                    SystemTopic,
                    SystemEvent::Cluster(ClusterEvent::NodeAdded(node_id)),
                    sys.actor_system(),
                )
                .await;
            });

            self.nodes.add(node)
        }
    }
}

#[async_trait]
impl Handler<RegisterNode> for RemoteRegistry {
    async fn handle(&mut self, message: RegisterNode, ctx: &mut ActorContext) {
        if ctx.system().is_remote() {
            PubSub::publish_locally(
                SystemTopic,
                SystemEvent::Cluster(ClusterEvent::NodeAdded(message.0.id)),
                ctx.system_mut(),
            )
            .await;
        }

        self.nodes.add(message.0);
    }
}

#[async_trait]
impl Handler<ClientWrite> for RemoteClientRegistry {
    async fn handle(&mut self, message: ClientWrite, ctx: &mut ActorContext) {
        let client_id = message.0;
        let message = message.1;

        if let Some(client) = self.clients.get_mut(&client_id) {
            client.send(message).await.expect("send client msg");
            trace!(ctx.log(), "writing data to client")
        } else {
            trace!(ctx.log(), "client {} not found", &client_id);
        }
    }
}

#[async_trait]
impl Handler<DeregisterClient> for RemoteClientRegistry {
    async fn handle(&mut self, message: DeregisterClient, ctx: &mut ActorContext) {
        let node_id = message.0;
        self.remove_client(node_id);
        trace!(ctx.log(), "removing client {}", &node_id);
    }
}

#[async_trait]
impl Handler<GetActorNode> for RemoteRegistry {
    async fn handle(&mut self, message: GetActorNode, ctx: &mut ActorContext) {
        let span = tracing::trace_span!(
            "RemoteRegistry::GetActorNode",
            actor_id = message.actor_id.as_str()
        );
        let _enter = span.enter();

        let id = message.actor_id;
        let current_system = self.system.as_ref().unwrap().node_id();
        let assigned_registry_node = self.nodes.get_by_key(&id).map(|n| n.id);

        let assigned_registry_node = assigned_registry_node.map_or_else(
            || {
                trace!(ctx.log(), "no nodes configured, assigning locally");
                current_system
            },
            |n| n,
        );

        trace!(ctx.log(), "{:?}", &self.nodes.get_all());

        if &assigned_registry_node == &current_system {
            trace!(ctx.log(), "searching locally, {}", current_system);
            let node = self.actors.get(&id).map(|s| *s);

            trace!(ctx.log(), "found: {:?}", &node);
            message.sender.send(node);
        } else {
            let system = self.system.as_ref().unwrap().clone();
            let sender = message.sender;

            trace!(
                ctx.log(),
                "asking remotely, current_sys={}, target_sys={}",
                current_system,
                assigned_registry_node
            );
            let log = ctx.log().clone();

            tokio::spawn(async move {
                let log = log;

                let span = tracing::trace_span!("RemoteRegistry::GetActorNode::Remote");
                let _enter = span.enter();

                let message_id = Uuid::new_v4();
                let system = system;
                let (res_tx, res_rx) = tokio::sync::oneshot::channel();

                trace!(&log, "remote request={}", message_id);
                system.push_request(message_id, res_tx).await;

                trace!(
                    &log,
                    "sending actor lookup request to={}",
                    assigned_registry_node
                );
                let trace_id = extract_trace_identifier(&span);
                system
                    .send_message(
                        assigned_registry_node,
                        SessionEvent::FindActor(FindActor {
                            message_id: message_id.to_string(),
                            actor_id: id,
                            trace_id,
                            ..FindActor::default()
                        }),
                    )
                    .await;

                trace!(&log, "lookup sent, waiting for result");
                match res_rx.await {
                    Ok(RemoteResponse::Ok(res)) => {
                        let res = ActorAddress::parse_from_bytes(&res);
                        match res {
                            Ok(res) => {
                                sender.send(if res.get_node_id().is_empty() {
                                    None
                                } else {
                                    Some(Uuid::from_str(res.get_node_id()).unwrap())
                                });
                            }
                            Err(e) => {
                                panic!("failed to decode message - {}", e.to_string());
                            }
                        }
                    }
                    _ => panic!("get actornode failed"),
                }
            });
        }
    }
}

#[async_trait]
impl Handler<RegisterActor> for RemoteRegistry {
    async fn handle(&mut self, message: RegisterActor, ctx: &mut ActorContext) {
        trace!(ctx.log(), "Registering actor: {:?}", &message);

        match message.node_id {
            Some(node_id) => {
                trace!(ctx.log(), "registering actor locally {}", node_id);
                self.actors.insert(message.actor_id, node_id);
            }

            None => {
                if let Some(system) = self.system.as_mut() {
                    let node_id = system.node_id();
                    let id = message.actor_id;

                    let assigned_registry_node =
                        self.nodes.get_by_key(&id).map_or_else(|| node_id, |n| n.id);

                    if &assigned_registry_node == &node_id {
                        trace!(
                            ctx.log(),
                            "registering actor locally {}",
                            assigned_registry_node
                        );
                        self.actors.insert(id, node_id);
                    } else {
                        let event = SessionEvent::RegisterActor(ActorAddress {
                            node_id: node_id.to_string(),
                            actor_id: id,
                            ..ActorAddress::default()
                        });
                        system.send_message(assigned_registry_node, event).await;
                    }
                }
            }
        }
    }
}

#[async_trait]
impl Handler<StreamEvent<SystemTopic>> for RemoteRegistry {
    async fn handle(&mut self, event: StreamEvent<SystemTopic>, ctx: &mut ActorContext) {
        match event {
            StreamEvent::Receive(msg) => match msg.as_ref() {
                SystemEvent::Cluster(_) => {
                    trace!(ctx.log(), "cluster event");
                    let system = self.system.as_ref().unwrap().clone();
                    let registry_ref = ctx.actor_ref::<Self>();

                    tokio::spawn(async move {
                        let sys = system;
                        let actor_ids = sys
                            .actor_system()
                            .scheduler()
                            .exec::<_, Vec<ActorId>>(|s| {
                                s.actors.keys().map(|k| k.clone()).collect()
                            })
                            .await
                            .expect("unable to get active actor ids from scheduler");

                        for actor_id in actor_ids {
                            registry_ref.notify(RegisterActor::new(actor_id, None));
                        }
                    });
                }
            },
            StreamEvent::Err => {
                warn!(ctx.log(), "received stream err");
            }
        }
    }
}

async fn connect_all(
    nodes: Vec<RemoteNode>,
    current_nodes: Vec<RemoteNode>,
    sys: &RemoteActorSystem,
) -> HashMap<Uuid, Option<RemoteClient>> {
    let mut clients = HashMap::new();
    for node in nodes {
        let addr = node.addr.to_string();
        match RemoteClient::connect(
            addr,
            Some(node.id),
            sys.clone(),
            Some(current_nodes.clone()),
            ClientType::Worker,
        )
        .await
        {
            Ok(client) => {
                trace!(sys.actor_system().log(), "connected to node");
                clients.insert(node.id, Some(client));
            }
            Err(_) => {
                clients.insert(node.id, None);
                warn!(
                    sys.actor_system().log(),
                    "failed to connect to discovered worker"
                );
            }
        }
    }

    clients
}
