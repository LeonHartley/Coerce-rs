use coerce::remote::system::{NodeId, RemoteActorSystem};
use std::collections::HashMap;

pub struct TestRemoteActorSystem {
    system: RemoteActorSystem,
    node_id: NodeId,
}

pub struct TestCluster {
    systems: HashMap<NodeId, TestRemoteActorSystem>,
}
