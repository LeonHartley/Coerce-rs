#[macro_use]
extern crate serde;
extern crate serde_json;

extern crate chrono;

#[macro_use]
extern crate async_trait;
extern crate hashring;

use coerce_remote::codec::json::JsonCodec;
use coerce_remote::transport::tcp::TcpTransport;

pub mod util;

#[tokio::test]
pub async fn test_remote_transport_tcp_json() {
    let transport = TcpTransport::new(JsonCodec::new());
}
