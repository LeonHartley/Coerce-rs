use crate::actor::ActorRefErr;

use crate::remote::handler::{ActorHandler, ActorMessageHandler};

use std::collections::HashMap;

pub mod clients;
pub mod message;
pub mod registry;

pub(crate) type BoxedActorHandler = Box<dyn ActorHandler + Send + Sync>;

pub(crate) type BoxedMessageHandler = Box<dyn ActorMessageHandler + Send + Sync>;

pub struct RemoteHandler {
    requests: HashMap<u64, RemoteRequest>,
}

impl RemoteHandler {
    pub fn push_request(&mut self, message_id: u64, request: RemoteRequest) {
        self.requests.insert(message_id, request);
    }

    pub fn pop_request(&mut self, message_id: u64) -> Option<RemoteRequest> {
        self.requests.remove(&message_id)
    }

    pub fn inflight_request_count(&self) -> usize {
        self.requests.len()
    }
}

pub struct RemoteRequest {
    pub res_tx: tokio::sync::oneshot::Sender<RemoteResponse>,
}

#[derive(Debug)]
pub enum RemoteResponse {
    Ok(Vec<u8>),
    Err(ActorRefErr),
}

impl RemoteResponse {
    pub fn is_ok(&self) -> bool {
        match self {
            &RemoteResponse::Ok(..) => true,
            _ => false,
        }
    }

    pub fn is_err(&self) -> bool {
        match self {
            &RemoteResponse::Err(..) => true,
            _ => false,
        }
    }

    pub fn into_result(self) -> Result<Vec<u8>, ActorRefErr> {
        match self {
            RemoteResponse::Ok(buff) => Ok(buff),
            RemoteResponse::Err(buff) => Err(buff),
        }
    }
}

impl RemoteHandler {
    pub fn new() -> RemoteHandler {
        RemoteHandler {
            requests: HashMap::new(),
        }
    }
}
