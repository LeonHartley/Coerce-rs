use crate::actor::message::{Handler, Message};
use crate::actor::remote::handler::{RemoteActorMessageHandler, RemoteMessageHandler};
use crate::actor::{get_actor, Actor, ActorId, ActorRef};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashMap;
use std::marker::PhantomData;
use crate::remote::actor::RemoteRegistry;
use crate::remote::handler::{RemoteMessageHandler, RemoteActorMessageHandler};

pub mod handler;
pub mod actor;
pub mod context;