use crate::actor::stream::{ChatMessage, Handshake};
use coerce::actor::message::EnvelopeType::Remote;
use coerce::actor::message::Message;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;
use tokio::task::JoinHandle;

use crate::actor::peer::message::{JoinChat, SendChatMessage};
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
use tungstenite::{Message as WebSocketMessage, Result};

pub type WebSocketReader = SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>;
pub type WebSocketWriter = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, WebSocketMessage>;

pub struct ChatClient {
    name: String,
    websocket_messages: Option<Receiver<WebSocketMessage>>,
    websocket_writer: WebSocketWriter,
    is_connected: bool,
    read_task: JoinHandle<()>,
}

impl ChatClient {
    pub async fn connect(url: &str, name: &str) -> Result<ChatClient> {
        let (socket, _) = connect_async(url).await?;

        let message = Handshake {
            name: name.to_string(),
        }
        .as_bytes()
        .unwrap();

        let (mut websocket_writer, mut websocket_reader) = socket.split();
        websocket_writer
            .send(WebSocketMessage::Binary(message))
            .await?;

        let (sender, receiver) = mpsc::channel::<WebSocketMessage>(128);
        let read_task = tokio::spawn(async move {
            let sender = sender;
            loop {
                match websocket_reader.next().await {
                    Some(msg) => match msg {
                        Ok(msg) => sender.send(msg).await.unwrap(),
                        Err(_e) => {
                            break;
                        }
                    },
                    None => {
                        break;
                    }
                }
            }
        });

        let name = name.to_string();
        Ok(ChatClient {
            name,
            websocket_writer,
            is_connected: false,
            websocket_messages: Some(receiver),
            read_task,
        })
    }

    pub fn take_reader(&mut self) -> Option<Receiver<WebSocketMessage>> {
        self.websocket_messages.take()
    }

    pub async fn read<M: Message>(&mut self) -> Option<M> {
        if self.websocket_messages.is_none() {
            return None;
        }

        let message = tokio::time::timeout(
            Duration::from_secs(3),
            self.websocket_messages.as_mut().unwrap().recv(),
        )
        .await;
        match message {
            Ok(message) => {
                if let Some(message) = message {
                    M::from_bytes(message.into_data()).map_or(None, |m| Some(m))
                } else {
                    None
                }
            }
            Err(_) => {
                error!(
                    "timeout whilst attempting to read next message (client_name={})",
                    &self.name
                );
                None
            }
        }
    }

    pub async fn write<M: Message>(&mut self, id: u8, message: M) {
        let mut buffer = message.as_bytes().unwrap();
        buffer.insert(0, id);

        let _ = self
            .websocket_writer
            .send(WebSocketMessage::Binary(buffer))
            .await
            .unwrap();
    }

    pub async fn join_chat(&mut self, chat_stream_id: String) {
        self.write(
            0,
            JoinChat {
                chat_stream_id,
                join_token: None,
            },
        )
        .await;
    }

    pub async fn chat(&mut self, chat_stream_id: String, message: String) {
        self.write(
            1,
            SendChatMessage(ChatMessage {
                chat_stream_id,
                message_id: None,
                sender: None,
                message,
            }),
        )
        .await;
    }
}

impl Drop for ChatClient {
    fn drop(&mut self) {
        self.read_task.abort();
    }
}
