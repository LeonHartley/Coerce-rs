use crate::remote::raft::RaftSystem;
use crate::remote::system::RemoteActorSystem;

impl RemoteActorSystem {
    pub fn raft(&self) -> Option<&RaftSystem> {
        self.inner.raft.as_ref().map(|s| s.as_ref())
    }
}
