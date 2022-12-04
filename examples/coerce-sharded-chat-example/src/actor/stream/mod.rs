use crate::actor::pubsub::{ChatReceive, ChatStreamTopic};
use coerce::actor::context::ActorContext;
use coerce::actor::message::{Handler, Message};
use coerce::actor::{ActorCreationErr, ActorFactory, ActorRecipe};
use coerce::persistent::journal::types::JournalTypes;
use coerce::persistent::{PersistentActor, Recover};
use coerce::remote::stream::pubsub::PubSub;

pub struct ChatStream {
    name: String,
    creator: String,
    messages: Vec<ChatMessage>,
    join_token: String,
    topic: ChatStreamTopic,
}

#[async_trait]
impl PersistentActor for ChatStream {
    fn persistence_key(&self, _ctx: &ActorContext) -> String {
        format!("ChatStream-{}", &self.name)
    }

    fn configure(types: &mut JournalTypes<Self>) {
        types.message::<ChatMessage>("ChatMessage");
    }

    async fn post_recovery(&mut self, _ctx: &mut ActorContext) {
        debug!(
            "ChatStream (name={}) created, recovered {} chat messages",
            &self.name,
            self.messages.len()
        );
    }
}

#[derive(Clone)]
pub struct ChatStreamFactory;

#[derive(Serialize, Deserialize)]
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
    pub chat_stream_id: String,
    pub message_id: Option<u64>,
    pub sender: Option<String>,
    pub message: String,
}

#[derive(Serialize, Deserialize, JsonMessage, Clone, Debug)]
#[result("()")]
pub struct Handshake {
    pub name: String,
}

#[async_trait]
impl Handler<Join> for ChatStream {
    async fn handle(&mut self, message: Join, _ctx: &mut ActorContext) -> JoinResult {
        info!(
            "user {} joined chat (chat_stream={})",
            &message.peer_name, &self.name
        );

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
        if self.persist(&message, ctx).await.is_ok() {
            self.messages.push(message.clone());

            info!(
                "user {} said \"{}\" (chat_stream={})",
                &message.sender.as_ref().unwrap(),
                &message.message,
                self.name
            );

            PubSub::publish(
                self.topic.clone(),
                ChatReceive::Message(message),
                ctx.system().remote(),
            )
            .await;
        }
    }
}

#[async_trait]
impl ActorFactory for ChatStreamFactory {
    type Actor = ChatStream;
    type Recipe = CreateChatStream;

    async fn create(&self, recipe: CreateChatStream) -> Result<ChatStream, ActorCreationErr> {
        debug!(
            "creating ChatStream actor, chat_stream_id={}, creator={}",
            &recipe.name, &recipe.creator
        );
        Ok(Self::Actor {
            name: recipe.name.clone(),
            creator: recipe.creator,
            messages: vec![],
            join_token: "todo: join-token".to_string(),
            topic: ChatStreamTopic(recipe.name),
        })
    }
}

#[async_trait]
impl Recover<ChatMessage> for ChatStream {
    async fn recover(&mut self, message: ChatMessage, _ctx: &mut ActorContext) {
        self.messages.push(message);
    }
}

impl ActorRecipe for CreateChatStream {
    fn read_from_bytes(bytes: &Vec<u8>) -> Option<Self> {
        serde_json::from_slice(bytes).map_or(None, |m| Some(m))
    }

    fn write_to_bytes(&self) -> Option<Vec<u8>> {
        serde_json::to_vec(&self).map_or(None, |m| Some(m))
    }
}
