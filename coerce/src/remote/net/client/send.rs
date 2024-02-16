use crate::actor::context::ActorContext;
use crate::actor::message::{Handler, Message};
use crate::remote::net::client::connect::Disconnected;
use crate::remote::net::client::{ClientState, ConnectionState, RemoteClient, RemoteClientErr};
use crate::remote::net::StreamData;
use bytes::{Bytes, BytesMut};
use futures::SinkExt;
use tokio::io::WriteHalf;
use tokio::net::TcpStream;
use tokio_util::codec::{FramedWrite, LengthDelimitedCodec};

pub struct Write<M: StreamData>(pub M);

impl<M: StreamData> Message for Write<M> {
    type Result = Result<(), RemoteClientErr>;
}

#[async_trait]
impl<M: StreamData> Handler<Write<M>> for RemoteClient {
    async fn handle(
        &mut self,
        message: Write<M>,
        ctx: &mut ActorContext,
    ) -> Result<(), RemoteClientErr> {
        self.write(message.0, ctx).await
    }
}

impl ConnectionState {
    pub async fn write(&mut self, bytes: Vec<u8>) -> Result<(), Option<Vec<u8>>> {
        if let Err(e) = write_bytes(Bytes::from(bytes), &mut self.write).await {
            match e {
                RemoteClientErr::StreamErr(_e) => Err(Some(vec![])),
                _ => Err(None),
            }
        } else {
            Err(None)
        }
    }
}

impl RemoteClient {
    pub async fn flush_buffered_writes(&mut self) {
        let connection_state = match &mut self.state {
            Some(ClientState::Connected(connection_state)) => connection_state,
            _ => return,
        };

        debug!(
            "flushing {} pending messages (addr={})",
            self.write_buffer.len(),
            &self.addr
        );

        while let Some(buffered_message) = self.write_buffer.pop_front() {
            let len = buffered_message.len();
            let bytes = Bytes::from(buffered_message);
            if let Ok(()) = write_bytes(bytes.clone(), &mut connection_state.write).await {
                self.write_buffer_bytes_total -= len;
            } else {
                self.write_buffer.push_front(bytes.to_vec());

                // write failed, no point trying again - break and reconnect/retry later
                break;
            }
        }
    }

    pub fn buffer_message(&mut self, message_bytes: Vec<u8>) {
        self.write_buffer_bytes_total += message_bytes.len();
        self.write_buffer.push_back(message_bytes);
    }

    pub async fn write<M: StreamData>(
        &mut self,
        message: M,
        ctx: &mut ActorContext,
    ) -> Result<(), RemoteClientErr>
    where
        M: Sync + Send,
    {
        if let Some(bytes) = message.write_to_bytes() {
            let mut buffer_message = None;

            let stream_write_error = match &mut self.state.as_mut().unwrap() {
                ClientState::Idle { .. } => {
                    buffer_message = Some(bytes);

                    debug!("attempt to write to addr={} but no connection is established, buffering message (total_buffered={})",
                        &self.addr,
                        self.write_buffer.len()
                    );

                    false
                }

                ClientState::Connected(state) => {
                    let bytes = Bytes::from(bytes);
                    if let Err(e) = write_bytes(bytes.clone(), &mut state.write).await {
                        match e {
                            RemoteClientErr::StreamErr(_e) => {
                                warn!("node {} (addr={}) is unreachable but marked as connected, buffering message (total_buffered={})",
                                    &state.identity.node.id,
                                    &self.addr,
                                    self.write_buffer.len());

                                buffer_message = Some(bytes.to_vec());

                                true
                            }
                            _ => false,
                        }
                    } else {
                        false
                    }
                }

                ClientState::Terminated => true,
            };

            if let Some(message_bytes) = buffer_message {
                self.buffer_message(message_bytes);
            }

            if stream_write_error {
                self.handle(Disconnected, ctx).await;
            }
        } else {
            return Err(RemoteClientErr::Encoding);
        }

        Ok(())
    }
}

pub(crate) async fn write_bytes(
    bytes: Bytes,
    writer: &mut FramedWrite<WriteHalf<TcpStream>, LengthDelimitedCodec>,
) -> Result<(), RemoteClientErr> {
    match writer.send(bytes).await {
        Ok(()) => Ok(()),
        Err(e) => Err(RemoteClientErr::StreamErr(e)),
    }
}
