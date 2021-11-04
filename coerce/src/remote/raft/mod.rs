use crate::actor::LocalActorRef;
use crate::remote::net::message::{ClientEvent, SessionEvent};
use crate::remote::net::proto::protocol::{ClientResult, RaftRequest as ProtoRaftRequest};
use crate::remote::net::server::session::{RemoteSessionStore, SessionWrite};
use crate::remote::net::StreamData;
use crate::remote::system::RemoteActorSystem;
use anyhow::Result;
use async_raft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use async_raft::{Config, Raft, RaftNetwork};
use memstore::{ClientRequest, ClientResponse, MemStore};
use serde::Serialize;
use std::sync::Arc;
use uuid::Uuid;

pub type RaftCore = Raft<ClientRequest, ClientResponse, RaftRouter, MemStore>;

pub struct RaftSystem {
    raft: RaftCore,
}

pub struct RaftRouter {
    system: RemoteActorSystem,
}

impl RaftSystem {
    pub fn new(system: RemoteActorSystem) -> Arc<RaftSystem> {
        let node_id = system.node_id();
        let cluster_name = "coerce".to_string();
        let config = Config::build(cluster_name)
            .heartbeat_interval(1000)
            .election_timeout_min(60000 * 30)
            .election_timeout_max(60000 * 60)
            .validate()
            .unwrap();

        Arc::new(RaftSystem {
            raft: Raft::new(
                node_id,
                Arc::new(config),
                Arc::new(RaftRouter::new(system)),
                Arc::new(MemStore::new(node_id)),
            ),
        })
    }

    pub fn core(&self) -> &RaftCore {
        &self.raft
    }
}

impl RaftRouter {
    pub fn new(system: RemoteActorSystem) -> RaftRouter {
        RaftRouter { system }
    }
}

#[derive(Debug)]
pub enum RaftRequest {
    AppendEntries(AppendEntriesRequest<ClientRequest>),
    InstallSnapshot(InstallSnapshotRequest),
    Vote(VoteRequest),
}

impl RaftSystem {
    pub async fn inbound_request(
        &self,
        proto_req: ProtoRaftRequest,
        session_id: Uuid,
        sessions: &LocalActorRef<RemoteSessionStore>,
    ) {
        let message_id = Uuid::parse_str(&proto_req.message_id).unwrap();
        let req = RaftRequest::from_proto(proto_req);

        info!(
            "inbound req, session={}, message_id={}",
            &session_id, &message_id
        );

        let result = match req {
            RaftRequest::AppendEntries(append_entries) => self
                .raft
                .append_entries(append_entries)
                .await
                .map(|res| res.write_to_bytes()),

            RaftRequest::InstallSnapshot(snapshot) => self
                .raft
                .install_snapshot(snapshot)
                .await
                .map(|res| res.write_to_bytes()),

            RaftRequest::Vote(vote) => self.raft.vote(vote).await.map(|res| res.write_to_bytes()),
        };

        match result {
            Ok(Some(result)) => {
                let result = ClientEvent::Result(ClientResult {
                    message_id: message_id.to_string(),
                    result,
                    ..Default::default()
                });

                sessions.send(SessionWrite(session_id, result)).await;
            }
            Err(e) => {
                // todo: send err
            }
            Ok(None) => {
                // todo: send serialisation err
            }
        }
    }
}

impl RaftRequest {
    fn request_type(&self) -> u32 {
        match &self {
            &RaftRequest::AppendEntries(_) => 1,
            &RaftRequest::InstallSnapshot(_) => 2,
            &RaftRequest::Vote(_) => 3,
        }
    }

    fn write_to_bytes(&self) -> Vec<u8> {
        // TODO: protobuf for the raft request (json is temporary)
        match self {
            RaftRequest::AppendEntries(append_entries) => {
                serde_json::to_vec(&append_entries).expect("serialise append_entries request")
            }
            RaftRequest::InstallSnapshot(install_snapshot) => {
                serde_json::to_vec(&install_snapshot).expect("serialise install_snapshot request")
            }
            RaftRequest::Vote(vote) => serde_json::to_vec(&vote).expect("serialise vote request"),
        }
    }

    fn from_proto(raft_req: ProtoRaftRequest) -> Self {
        match raft_req.request_type {
            1 => RaftRequest::AppendEntries(serde_json::from_slice(&raft_req.payload).unwrap()),
            2 => RaftRequest::InstallSnapshot(serde_json::from_slice(&raft_req.payload).unwrap()),
            3 => RaftRequest::Vote(serde_json::from_slice(&raft_req.payload).unwrap()),
            i => panic!("unexpected payload type: {}", i),
        }
    }

    pub fn to_proto(self, message_id: Uuid) -> ProtoRaftRequest {
        let request_type = self.request_type();
        let payload = self.write_to_bytes();
        let message_id = message_id.to_string();

        ProtoRaftRequest {
            request_type,
            payload,
            message_id,
            ..Default::default()
        }
    }
}

#[async_trait]
impl RaftNetwork<ClientRequest> for RaftRouter {
    async fn append_entries(
        &self,
        target: u64,
        rpc: AppendEntriesRequest<ClientRequest>,
    ) -> Result<AppendEntriesResponse> {
        let message_id = Uuid::new_v4();
        let event = SessionEvent::Raft(RaftRequest::AppendEntries(rpc).to_proto(message_id));

        info!("append_entries rpc, id={}", &message_id);
        let res = self.system.node_rpc(message_id, event, target).await?;
        info!("append_entries rpc, id={} is complete", &message_id);
        Ok(res)
    }

    async fn install_snapshot(
        &self,
        target: u64,
        rpc: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse> {
        let message_id = Uuid::new_v4();
        let event = SessionEvent::Raft(RaftRequest::InstallSnapshot(rpc).to_proto(message_id));

        info!("install_snapshot rpc, id={}", &message_id);
        let res = self.system.node_rpc(message_id, event, target).await?;
        info!("install_snapshot rpc, id={} is complete", &message_id);
        Ok(res)
    }

    async fn vote(&self, target: u64, rpc: VoteRequest) -> Result<VoteResponse> {
        let message_id = Uuid::new_v4();
        let event = SessionEvent::Raft(RaftRequest::Vote(rpc).to_proto(message_id));

        info!("vote rpc, id={}", &message_id);
        let res = self.system.node_rpc(message_id, event, target).await?;
        info!("vote rpc, id={} is complete", &message_id);
        Ok(res)
    }
}

impl StreamData for InstallSnapshotResponse {
    fn read_from_bytes(data: Vec<u8>) -> Option<Self> {
        serde_json::from_slice(&data).map_or(None, |s| s)
    }

    fn write_to_bytes(&self) -> Option<Vec<u8>> {
        serde_json::to_vec(&self).map_or(None, |s| Some(s))
    }
}

impl StreamData for AppendEntriesResponse {
    fn read_from_bytes(data: Vec<u8>) -> Option<Self> {
        serde_json::from_slice(&data).map_or(None, |s| s)
    }

    fn write_to_bytes(&self) -> Option<Vec<u8>> {
        serde_json::to_vec(&self).map_or(None, |s| Some(s))
    }
}

impl StreamData for VoteResponse {
    fn read_from_bytes(data: Vec<u8>) -> Option<Self> {
        serde_json::from_slice(&data).map_or(None, |s| s)
    }

    fn write_to_bytes(&self) -> Option<Vec<u8>> {
        serde_json::to_vec(&self).map_or(None, |s| Some(s))
    }
}
