// use std::net::SocketAddr;
// use std::sync::Arc;
//
// pub struct AlertTopic;
//
// #[derive(Debug)]
// pub enum SystemAlert {
//     // Fired when a client has attempted to connect to this system as a cluster peer,
//     // sent a valid identify payload, but the token failed validation and/or authentication.
//     AuthenticationFailure(Arc<AuthenticationFailure>),
// }
//
// pub struct AuthenticationFailure {
//     client_addr: SocketAddr,
//     token: Vec<u8>,
// }
//
// impl AuthenticationFailure {
//     pub fn new(client_addr: SocketAddr, token: Vec<u8>) -> AuthenticationFailure {
//         Self { client_addr, token }
//     }
// }
