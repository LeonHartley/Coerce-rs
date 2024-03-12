// use crate::remote::cluster::node::RemoteNodeState;
// use crate::remote::stream::pubsub::Topic;
//
// pub struct HeartbeatTopic;
//
// impl Topic for HeartbeatTopic {
//     type Message = HeartbeatState;
//
//     fn topic_name() -> &'static str {
//         "heartbeat"
//     }
// }
//
// pub struct HeartbeatState {
//     nodes: Vec<RemoteNodeState>,
// }
