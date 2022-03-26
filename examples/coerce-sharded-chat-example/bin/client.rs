use coerce_sharded_chat_example::websocket::client::ChatClient;
use tokio::io::BufReader;

#[tokio::main]
pub async fn main() {
    let websocket_url = std::env::args().nth(1).expect("no websocket_url given");
    let client_name = std::env::args().nth(2).expect("no client_name given");

    let mut client = ChatClient::connect(&websocket_url, &client_name)
        .await
        .expect(&format!("connect to websocket (url={})", &websocket_url));
}
