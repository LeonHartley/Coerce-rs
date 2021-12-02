use crate::actor::message::{Envelope, Handler, Message};
use crate::actor::{Actor, ActorRefErr, LocalActorRef};
use crate::remote::cluster::sharding::host::ShardHost;
use std::sync::Arc;

pub mod coordinator;
pub mod host;
pub mod proto;
pub mod shard;

// pub struct Sharded<A: Actor> {
//     core: Arc<ShardedCore>,
// }
//
// struct ShardedCore {
//     host: LocalActorRef<ShardHost>,
// }
//
// impl<A: Actor> Sharded<A> {
//     pub async fn send<M: Message>(&self, message: M) -> Result<(), ActorRefErr>
//     where
//         A: Handler<M>,
//     {
//         let message =  match message.as_remote_envelope() {
//             Ok(Envelope::Remote(b)) => b,
//             _ => return Err(ActorRefErr::ActorUnavailable)
//         };
//
//         let entity_request = self.core.host
//             .notify(EntityRequest {
//                 actor_id: "leon".to_string(),
//                 message_type: "SetStatusRequest".to_string(),
//                 message,
//                 recipe: Some(vec![]),
//                 result_channel: None,
//             })
//             .await;
//     }
// }
