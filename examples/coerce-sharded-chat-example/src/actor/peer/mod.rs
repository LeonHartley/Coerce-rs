use crate::actor::pubsub::{ChatReceive, ChatStreamTopic};
use crate::actor::stream::{
    ChatMessage, ChatStream, ChatStreamFactory, CreateChatStream, Join, JoinResult,
};
use coerce::actor::context::{attach_stream, ActorContext, StreamAttachmentOptions};
use coerce::actor::message::{Handler, Message};
use coerce::actor::{Actor, CoreActorRef};
use coerce::remote::stream::pubsub::{PubSub, Receive, Subscription};
use coerce::sharding::{Sharded, Sharding};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::SinkExt;
use std::collections::HashMap;
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio_tungstenite::WebSocketStream;

use tungstenite::Message as WebSocketMessage;

pub type WebSocketReader = SplitStream<WebSocketStream<TcpStream>>;
pub type WebSocketWriter = SplitSink<WebSocketStream<TcpStream>, WebSocketMessage>;

pub mod message;

pub use message::*;

pub struct Peer {
    name: String,
    client_address: SocketAddr,
    websocket_reader: Option<WebSocketReader>,
    websocket_writer: WebSocketWriter,
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
            chat_streams: HashMap::new(),
        }
    }

    pub async fn write<M: Message>(&mut self, id: Option<u8>, message: M) {
        if let Err(_e) = self
            .websocket_writer
            .send(WebSocketMessage::Binary(
                write_outbound_message(id, message).unwrap(),
            ))
            .await
        {
            // todo: log err?
        }
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
                WebSocketMessage::Frame(_) => None,
            },
        )
    }
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
impl Handler<Receive<ChatStreamTopic>> for Peer {
    async fn handle(&mut self, message: Receive<ChatStreamTopic>, _ctx: &mut ActorContext) {
        match message.0.as_ref() {
            ChatReceive::Message(chat_message) => {
                let sender = chat_message.sender.clone();
                let message = chat_message.message.clone();

                trace!(
                    "user={} received chat message {} from {:?}",
                    &self.name,
                    &message,
                    &sender
                );

                self.write(None, chat_message.clone()).await;
            }
        }
    }
}

#[async_trait]
impl Handler<JoinChat> for Peer {
    async fn handle(&mut self, message: JoinChat, ctx: &mut ActorContext) {
        let chat_stream_id = message.chat_stream_id;
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
                    chat_stream_id: chat_stream_id.clone(),
                    message_id: None,
                    sender: Some("Coerce".to_string()),
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
    async fn handle(&mut self, message: SendChatMessage, _ctx: &mut ActorContext) {
        if let Some((chat_stream, _)) = self.chat_streams.get(&message.0.chat_stream_id) {
            let mut message = message.0;
            message.sender = Some(self.name.clone());

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
