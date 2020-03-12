use crate::context::RemoteActorContext;
use coerce_rt::actor::context::ActorHandlerContext;

const ATTACHMENT_REMOTE_CTX: &str = "remote-ctx";

pub trait ActorHandlerContextExt {
    fn remote_ctx(&self) -> Option<&RemoteActorContext>;

    fn remote_ctx_mut(&mut self) -> Option<&mut RemoteActorContext>;
}

impl ActorHandlerContextExt for ActorHandlerContext {
    fn remote_ctx(&self) -> Option<&RemoteActorContext> {
        self.attachment(ATTACHMENT_REMOTE_CTX)
    }

    fn remote_ctx_mut(&mut self) -> Option<&mut RemoteActorContext> {
        self.attachment_mut(ATTACHMENT_REMOTE_CTX)
    }
}
