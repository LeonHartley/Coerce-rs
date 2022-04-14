use crate::actor::pubsub::{ChatStreamEvent, ChatStreamTopic};
use crate::actor::stream::{
    ChatMessage, ChatStream, ChatStreamFactory, CreateChatStream, Join, JoinResult,
};
use coerce::actor::context::{attach_stream, ActorContext, StreamAttachmentOptions};
use coerce::actor::message::EnvelopeType::Remote;
use coerce::actor::message::{Handler, Message};
use coerce::actor::{Actor, ActorRefErr, CoreActorRef};
use coerce::remote::cluster::sharding::{Sharded, Sharding};
use coerce::remote::stream::pubsub::{PubSub, StreamEvent, Subscription};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use tungstenite::Message as WebSocketMessage;

pub type WebSocketReader = SplitStream<WebSocketStream<TcpStream>>;
pub type WebSocketWriter = SplitSink<WebSocketStream<TcpStream>, WebSocketMessage>;

pub struct Peer {
    name: String,
    client_address: SocketAddr,
    websocket_reader: Option<WebSocketReader>,
    websocket_writer: WebSocketWriter,
    stream_subscriptions: Vec<Subscription>,
    chat_stream_sharding: Sharding<ChatStreamFactory>,
    chat_streams: HashMap<String, (Sharded<ChatStream>, Subscription)>,
}

impl Peer {
    pub fn new(
        name: String,
        client_address: SocketAddr,
        websocket_reader: WebSocketReader,
        websocket_writer: WebSocketWriter,
        chat_stream_sharding: Sharding<ChatStreamFactory>,
    ) -> Self {
        let websocket_reader = Some(websocket_reader);

        Self {
            name,
            client_address,
            websocket_reader,
            websocket_writer,
            chat_stream_sharding,
            stream_subscriptions: vec![],
            chat_streams: HashMap::new(),
        }
    }

    pub async fn write<M: Message>(&mut self, id: Option<u8>, message: M) {
        self.websocket_writer
            .send(WebSocketMessage::Binary(
                write_outbound_message(id, message).unwrap(),
            ))
            .await;
    }
}

#[async_trait]
impl Actor for Peer {
    async fn started(&mut self, ctx: &mut ActorContext) {
        let reader = self.websocket_reader.take().unwrap();

        attach_stream(
            self.actor_ref(ctx),
            reader,
            StreamAttachmentOptions::default(),
            |msg| match msg {
                WebSocketMessage::Text(data) => parse_inbound_message(data.into_bytes()),
                WebSocketMessage::Binary(data) => parse_inbound_message(data),
                WebSocketMessage::Ping(_) => None,
                WebSocketMessage::Pong(_) => None,
                WebSocketMessage::Close(_) => Some(ClientEvent::Close),
            },
        )
    }
}

enum ClientEvent {
    Join(JoinChat),
    Chat(SendChatMessage),
    Leave(LeaveChat),
    Close,
}

#[derive(Serialize, Deserialize, coerce_macros::JsonMessage, Clone)]
#[result("()")]
pub struct SendChatMessage {
    pub chat_stream: String,
    pub message: ChatMessage,
}

#[derive(Serialize, Deserialize, coerce_macros::JsonMessage, Clone)]
#[result("()")]
pub struct JoinChat {
    pub stream_name: String,
    pub join_token: Option<String>,
}

#[derive(Serialize, Deserialize, coerce_macros::JsonMessage, Clone)]
#[result("()")]
pub struct LeaveChat(String);

impl Message for ClientEvent {
    type Result = ();
}

#[async_trait]
impl Handler<ClientEvent> for Peer {
    async fn handle(&mut self, message: ClientEvent, ctx: &mut ActorContext) {
        match message {
            ClientEvent::Join(join_chat) => self.handle(join_chat, ctx).await,
            ClientEvent::Chat(chat) => self.handle(chat, ctx).await,
            ClientEvent::Leave(leave_chat) => self.handle(leave_chat, ctx).await,
            ClientEvent::Close => {
                let _ = ctx.boxed_actor_ref().notify_stop();
            }
        }
    }
}

#[async_trait]
impl Handler<StreamEvent<ChatStreamTopic>> for Peer {
    async fn handle(&mut self, message: StreamEvent<ChatStreamTopic>, _ctx: &mut ActorContext) {
        match message {
            StreamEvent::Receive(chat_stream_event) => match chat_stream_event.as_ref() {
                ChatStreamEvent::Message(chat_message) => {
                    let sender = chat_message.sender.clone();
                    let message = chat_message.message.clone();

                    trace!(
                        "user={} received chat message {} from {}",
                        &self.name,
                        &message,
                        &sender
                    );

                    self.write(None, ChatMessage { sender, message }).await;
                }
            },
            StreamEvent::Err => {}
        }
    }
}

#[async_trait]
impl Handler<JoinChat> for Peer {
    async fn handle(&mut self, message: JoinChat, ctx: &mut ActorContext) {
        let chat_stream_id = message.stream_name;
        let chat_stream = self.chat_stream_sharding.get(
            chat_stream_id.clone(),
            Some(CreateChatStream {
                name: chat_stream_id.clone(),
                creator: self.name.clone(),
            }),
        );

        debug!(
            "user {} joining chat (chat_stream={})",
            &self.name, &chat_stream_id
        );
        let result = chat_stream
            .send(Join {
                peer_name: self.name.clone(),
                token: message.join_token,
            })
            .await;

        if let Ok(JoinResult::Ok {
            message_history,
            creator,
            ..
        }) = result
        {
            let topic = ChatStreamTopic(chat_stream_id.clone());
            let subscription = PubSub::subscribe::<Self, _>(topic, ctx).await;
            let chat_stream = (
                chat_stream,
                subscription.expect("ChatStream PubSub subscription"),
            );

            info!(
                "user {} joined chat (chat_stream={}, creator={}), history = {:?}",
                &self.name, &chat_stream_id, &creator, message_history
            );

            for message in message_history {
                self.write(None, message).await;
            }

            self.write(
                None,
                ChatMessage {
                    sender: "Coerce".to_string(),
                    message: format!("Welcome to {}, say hello!", &chat_stream_id),
                },
            )
            .await;

            self.chat_streams.insert(chat_stream_id, chat_stream);
        } else {
            error!(
                "user {} failed to join chat (stream={})",
                &self.name, &chat_stream_id
            );
        }
    }
}

#[async_trait]
impl Handler<SendChatMessage> for Peer {
    async fn handle(&mut self, message: SendChatMessage, ctx: &mut ActorContext) {
        if let Some((chat_stream, _)) = self.chat_streams.get(&message.chat_stream) {
            let mut message = message.message;
            message.sender = self.name.clone();

            chat_stream.send(message).await.expect("send chat message")
        }
    }
}

#[async_trait]
impl Handler<LeaveChat> for Peer {
    async fn handle(&mut self, message: LeaveChat, _ctx: &mut ActorContext) {
        if let Some((_, mut subscription)) = self.chat_streams.remove(&message.0) {
            subscription.unsubscribe();
        }
    }
}

fn parse_inbound_message(data: Vec<u8>) -> Option<ClientEvent> {
    match data.split_first() {
        Some((message_type, message)) => match *message_type {
            0 => Some(ClientEvent::Join(
                JoinChat::from_remote_envelope(message.to_vec()).unwrap(),
            )),
            1 => Some(ClientEvent::Chat(
                SendChatMessage::from_remote_envelope(message.to_vec()).unwrap(),
            )),
            2 => Some(ClientEvent::Leave(
                LeaveChat::from_remote_envelope(message.to_vec()).unwrap(),
            )),
            _ => Some(ClientEvent::Close),
        },
        _ => Some(ClientEvent::Close),
    }
}

fn write_outbound_message<M: Message>(id: Option<u8>, event: M) -> Option<Vec<u8>> {
    event.into_envelope(Remote).map_or(None, |e| {
        let mut bytes = e.into_bytes();

        if let Some(id) = id {
            bytes.insert(0, id);
        }

        Some(bytes)
    })
}
