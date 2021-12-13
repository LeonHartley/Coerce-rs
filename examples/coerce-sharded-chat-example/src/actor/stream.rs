use crate::actor::pubsub::{ChatStreamEvent, ChatStreamTopic};
use coerce::actor::context::ActorContext;
use coerce::actor::message::{Handler, Message};
use coerce::actor::{Actor, ActorCreationErr, ActorFactory, ActorRecipe};
use coerce::remote::net::StreamData;
use coerce::remote::stream::pubsub::{PubSub, Topic};

pub struct ChatStream {
    name: String,
    creator: String,
    messages: Vec<ChatMessage>,
    join_token: String,
    topic: ChatStreamTopic,
}

#[derive(Clone)]
pub struct ChatStreamFactory;

pub struct CreateChatStream {
    pub name: String,
    pub creator: String,
}

#[derive(Serialize, Deserialize, JsonMessage)]
#[result(JoinResult)]
pub struct Join {
    pub peer_name: String,
    pub token: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub enum JoinResult {
    Ok {
        creator: String,
        message_history: Vec<ChatMessage>,
        token: String,
    },
    NameCollision,
}

#[derive(Serialize, Deserialize, JsonMessage, Clone, Debug)]
#[result("()")]
pub struct ChatMessage {
    pub sender: String,
    pub message: String,
}

#[Derive(Serialize, Deserialize, JsonMessage, Clone, Debug)]
#[result("()")]
pub struct Handshake {
    pub name: String,
}

impl Actor for ChatStream {}

#[async_trait]
impl Handler<Join> for ChatStream {
    async fn handle(&mut self, message: Join, _ctx: &mut ActorContext) -> JoinResult {
        JoinResult::Ok {
            creator: self.creator.clone(),
            message_history: self.messages.clone(),
            token: self.join_token.clone(),
        }
    }
}

#[async_trait]
impl Handler<ChatMessage> for ChatStream {
    async fn handle(&mut self, message: ChatMessage, ctx: &mut ActorContext) {
        self.messages.push(message.clone());

        PubSub::publish(
            self.topic.clone(),
            ChatStreamEvent::Message(message),
            ctx.system(),
        )
        .await;
    }
}

#[async_trait]
impl ActorFactory for ChatStreamFactory {
    type Actor = ChatStream;
    type Recipe = CreateChatStream;

    async fn create(&self, recipe: CreateChatStream) -> Result<ChatStream, ActorCreationErr> {
        Ok(Self::Actor {
            name: recipe.name.clone(),
            creator: recipe.creator,
            messages: vec![],
            join_token: "todo: join-token".to_string(),
            topic: ChatStreamTopic(recipe.name),
        })
    }
}

impl ActorRecipe for CreateChatStream {
    fn read_from_bytes(bytes: Vec<u8>) -> Option<Self> {
        None
    }

    fn write_to_bytes(&self) -> Option<Vec<u8>> {
        None
    }
}
