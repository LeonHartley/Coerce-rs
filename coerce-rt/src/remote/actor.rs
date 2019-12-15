use std::collections::HashMap;
use crate::actor::Actor;
use crate::remote::handler::RemoteMessageHandler;

pub struct RemoteRegistry {
    nodes:
    actors:
}

pub struct RemoteHandler {
    handlers: HashMap<String, Box<dyn RemoteMessageHandler>>,
}

impl Actor for RemoteRegistry {

}

impl Actor for RemoteHandler {

}