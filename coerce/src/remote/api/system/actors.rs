use crate::actor::describe;
use crate::remote::system::RemoteActorSystem;
use axum::extract::Query;
use axum::response::IntoResponse;
use axum::Json;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;
use crate::actor::describe::DescribeOptions;


#[derive(Serialize, Deserialize)]
pub struct DescribeAll {
    pub max_depth: Option<usize>,
    pub max_children: Option<usize>,
    pub child_describe_timeout: Option<Duration>,

    #[serde(default = "child_describe_attached_default")]
    pub child_describe_attached: bool,
}

fn child_describe_attached_default() -> bool {
    true
}

#[derive(Serialize, Deserialize)]
pub struct Actors {
    actors: Vec<describe::DescribeResult>,
}

impl From<DescribeAll> for DescribeOptions {
    fn from(value: DescribeAll) -> Self {
        Self {
            max_depth: value.max_depth,
            max_children: None,
            child_describe_timeout: None,
            child_describe_attached: false
        }
    }
}

pub(crate) async fn describe_all(
    system: RemoteActorSystem,
    options: Query<DescribeAll>,
) -> impl IntoResponse {
    // TODO: pass in through query parameters or something?
    let options = Arc::new(options.0.into());

    let (tx, rx) = oneshot::channel();
    let _ = system.actor_system().scheduler().notify(describe::DescribeAll {
        options,
        sender: tx,
    });

    let actors = rx.await.unwrap();
    Json(Actors { actors })
}
