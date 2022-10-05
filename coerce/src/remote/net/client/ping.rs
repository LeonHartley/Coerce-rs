use chrono::Utc;
use protobuf::Message as ProtoMessage;
use std::time::Instant;
use tokio::sync::oneshot;

use uuid::Uuid;

use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message};
use crate::actor::scheduler::timer::TimerTick;
use crate::remote::actor::RemoteResponse;
use crate::remote::cluster::discovery::Forget;

use crate::remote::heartbeat::{NodePing, PingResult};
use crate::remote::net::client::{ClientState, RemoteClient};
use crate::remote::net::message::SessionEvent;
use crate::remote::net::proto::network::{PingEvent, PongEvent};

#[derive(Clone)]
pub struct PingTick;

impl Message for PingTick {
    type Result = ();
}

impl TimerTick for PingTick {}

#[async_trait]
impl Handler<PingTick> for RemoteClient {
    async fn handle(&mut self, _: PingTick, ctx: &mut ActorContext) {
        let remote = ctx.system().remote_owned();
        let heartbeat = remote.heartbeat().clone();

        info!("ping tick, client_addr={}", &self.addr);

        let node_id = if let Some(state) = &self.state {
            match state {
                ClientState::Connected(state) => state.identity.node.id,
                _ => {
                    if let Some(node_id) = self.node_id {
                        let _ = heartbeat.notify(NodePing(node_id, PingResult::Disconnected));
                    }

                    let _ = remote.node_discovery().notify(Forget(self.addr.clone()));
                    if let Some(ping_timer) = self.ping_timer.take() {
                        let _ = ping_timer.stop();

                        debug!(
                            "client disconnected (addr={}), stopped ping timer",
                            &self.addr
                        );
                    }

                    return;
                }
            }
        } else {
            info!(
                "ping tick cancelled, client not connected - client_addr={}",
                &self.addr
            );
            return;
        };

        let (res_tx, res_rx) = oneshot::channel();
        let message_id = Uuid::new_v4();
        remote.push_request(message_id, res_tx);

        let ping_event = SessionEvent::Ping(PingEvent {
            message_id: message_id.to_string(),
            node_id: remote.node_id(),
            ..PingEvent::default()
        });

        let client_addr = self.addr.clone();
        let ping_start = Instant::now();
        if self.write(ping_event, ctx).await.is_ok() {
            tokio::spawn(async move {
                let timeout = remote.config().heartbeat_config().ping_timeout;

                let ping_result_receiver = res_rx;
                let ping_result = match tokio::time::timeout(timeout, ping_result_receiver).await {
                    Ok(res) => match res {
                        Ok(pong) => match pong {
                            RemoteResponse::Ok(pong_bytes) => {
                                let ping_end = ping_start.elapsed();
                                let pong = PongEvent::parse_from_bytes(&pong_bytes).unwrap();
                                PingResult::Ok(pong, ping_end, Utc::now())
                            }
                            RemoteResponse::Err(_err_bytes) => PingResult::Err,
                        },
                        Err(_e) => PingResult::Err,
                    },
                    Err(_e) => PingResult::Timeout,
                };

                let ping = NodePing(node_id, ping_result);
                info!(
                    "ping complete - client_addr={}, nodePing = {:?}",
                    client_addr, &ping
                );
                let _ = heartbeat.notify(ping);
            });
        } else {
            warn!("(addr={}) ping write failed", &self.addr);
        }
    }
}
