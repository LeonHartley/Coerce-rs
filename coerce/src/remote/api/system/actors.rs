use crate::actor::describe;
use crate::remote::system::RemoteActorSystem;
use axum::extract::Query;
use axum::response::IntoResponse;
use axum::Json;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct GetAll {
    pub max_depth: Option<usize>,
    pub max_children: Option<usize>,
    pub child_describe_timeout: Option<Duration>,

    #[serde(default = "child_describe_attached_default")]
    pub child_describe_attached: bool,
}

fn child_describe_attached_default() -> bool {
    true
}

#[derive(Serialize, Deserialize, ToSchema)]
pub struct Actors {
    actors: Vec<ActorDescription>,
}

#[derive(Serialize, Deserialize, ToSchema, Debug)]
pub enum Status {
    Ok,
    Err(String),
    Timeout,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq)]
pub enum ActorTags {
    None,
    Tag(String),
    Tags(Vec<String>),
}

#[derive(Serialize, Deserialize, ToSchema, Debug)]
pub struct ActorDescription {
    pub actor_id: String,
    pub path: String,
    pub status: Status,
    pub actor_type_name: String,
    pub actor_context_id: Option<u64>,
    pub tags: Option<ActorTags>,
    pub supervised: Option<SupervisedDescription>,
}

#[derive(Serialize, Deserialize, ToSchema, Debug)]
pub struct SupervisedDescription {
    pub actors: Vec<ActorDescription>,
}

impl Default for ActorDescription {
    fn default() -> Self {
        Self {
            actor_id: String::default(),
            path: String::default(),
            status: Status::Ok,
            actor_type_name: String::default(),
            actor_context_id: None,
            tags: None,
            supervised: None,
        }
    }
}

#[utoipa::path(
    get,
    path = "/actors/all",
    responses(
        (status = 200, description = "Actor system hierarchy description", body = Actors),
    ),
    params(
        ("max_depth" = usize, Path, description = "Maximum level into the actor hierarchy to be described"),
        ("max_children" = usize, Path, description = "Maximum children to describe, per top-level actor"),
    )
)]
pub(super) async fn get_all(
    system: RemoteActorSystem,
    Query(options): Query<GetAll>,
) -> impl IntoResponse {
    // TODO: pass in through query parameters or something?
    let options = Arc::new(options.into());

    let (tx, rx) = oneshot::channel();
    let _ = system
        .actor_system()
        .scheduler()
        .notify(describe::DescribeAll {
            options,
            sender: tx,
        });

    let actors = rx.await.unwrap();
    Json(Actors {
        actors: actors.into_iter().map(|a| a.into()).collect(),
    })
}

impl From<GetAll> for describe::DescribeOptions {
    fn from(value: GetAll) -> Self {
        Self {
            max_depth: value.max_depth,
            max_children: value.max_children,
            child_describe_timeout: value.child_describe_timeout,
            child_describe_attached: value.child_describe_attached,
        }
    }
}

impl From<describe::DescribeResult> for ActorDescription {
    fn from(value: describe::DescribeResult) -> Self {
        match value {
            describe::DescribeResult::Ok(value) => Self {
                actor_id: value.actor_id.to_string(),
                path: value.path.to_string(),
                status: Status::Ok,
                actor_type_name: value.actor_type_name,
                actor_context_id: Some(value.actor_context_id),
                tags: Some(value.tags.into()),
                supervised: value.supervised.map(|s| s.into()),
            },

            describe::DescribeResult::Err {
                error,
                actor_id,
                actor_path,
                actor_type,
            } => Self {
                actor_id: actor_id.to_string(),
                path: format!("{}/{}", actor_path, actor_id),
                status: Status::Err(format!("{}", error)),
                actor_type_name: actor_type.to_string(),
                ..Default::default()
            },

            describe::DescribeResult::Timeout {
                actor_id,
                actor_path,
                actor_type,
            } => Self {
                actor_id: actor_id.to_string(),
                path: format!("{}/{}", actor_path, actor_id),
                status: Status::Timeout,
                actor_type_name: actor_type.to_string(),
                ..Default::default()
            },
        }
    }
}

impl From<describe::SupervisedDescription> for SupervisedDescription {
    fn from(value: describe::SupervisedDescription) -> Self {
        Self {
            actors: value.actors.into_iter().map(|a| a.into()).collect(),
        }
    }
}

impl From<crate::actor::ActorTags> for ActorTags {
    fn from(value: crate::actor::ActorTags) -> Self {
        match value {
            crate::actor::ActorTags::None => Self::None,
            crate::actor::ActorTags::Tag(tag) => Self::Tag(tag.to_string()),
            crate::actor::ActorTags::Tags(tags) => {
                Self::Tags(tags.into_iter().map(|t| t.to_string()).collect())
            }
        }
    }
}
