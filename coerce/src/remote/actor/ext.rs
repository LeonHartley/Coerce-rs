use crate::actor::context::ActorContext;
use crate::remote::context::RemoteActorSystem;

const ATTACHMENT_REMOTE_CTX: &str = "remote-ctx";

pub trait ActorContextExt {
    fn remote_ctx(&self) -> Option<&RemoteActorSystem>;

    fn remote_ctx_mut(&mut self) -> Option<&mut RemoteActorSystem>;
}

impl ActorContextExt for ActorContext {
    fn remote_ctx(&self) -> Option<&RemoteActorSystem> {
        self.attachment(ATTACHMENT_REMOTE_CTX)
    }

    fn remote_ctx_mut(&mut self) -> Option<&mut RemoteActorSystem> {
        self.attachment_mut(ATTACHMENT_REMOTE_CTX)
    }
}
