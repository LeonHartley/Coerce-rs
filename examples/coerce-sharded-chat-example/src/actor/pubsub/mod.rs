use crate::actor::stream::ChatMessage;
use coerce::remote::net::StreamData;
use coerce::remote::stream::pubsub::Topic;

#[derive(Serialize, Deserialize)]
pub enum ChatReceive {
    Message(ChatMessage),
}

#[derive(Clone)]
pub struct ChatStreamTopic(pub String);

impl Topic for ChatStreamTopic {
    type Message = ChatReceive;

    fn topic_name() -> &'static str {
        "ChatStream"
    }

    fn key(&self) -> String {
        self.0.clone()
    }
}

impl StreamData for ChatReceive {
    fn read_from_bytes(data: Vec<u8>) -> Option<Self> {
        serde_json::from_slice(&data).unwrap()
    }

    fn write_to_bytes(&self) -> Option<Vec<u8>> {
        Some(serde_json::to_vec(self).unwrap())
    }
}
